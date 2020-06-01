#https://dash.plotly.com/live-updates

import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output

from consumer import MyKafkaConnect


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div([
        html.Div([
            html.H4('TERRA Satellite Live Feed'),
            html.Div(id='terra-text'),
            dcc.Graph(id='terra-graph')
            ], className="four columns"),
        html.Div([
            html.H4('AQUA Satellite Live Feed'),
            html.Div(id='aqua-text'),
            dcc.Graph(id='aqua-graph')
            ], className="four columns"),
        html.Div([
            html.H4('AURA Satellite Live Feed'),
            html.Div(id='aura-text'),
            dcc.Graph(id='aura-graph')
            ], className="four columns"),
        dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
    ], className="row")
)

def create_graphs(topic, live_update_text, live_update_graph):

    connect = MyKafkaConnect(topic=topic, group='test_group')

    @app.callback(Output(live_update_text, 'children'),
                  [Input('interval-component', 'n_intervals')])
    def update_metrics_terra(n):
        lon, lat, alt = connect.get_last()

        print('update metrics')

        style = {'padding': '5px', 'fontSize': '15px'}
        return [
            html.Span('Longitude: {0:.2f}'.format(lon), style=style),
            html.Span('Latitude: {0:.2f}'.format(lat), style=style),
            html.Span('Altitude: {0:0.2f}'.format(alt), style=style)
        ]


    # Multiple components can update everytime interval gets fired.
    @app.callback(Output(live_update_graph, 'figure'),
                  [Input('interval-component', 'n_intervals')])
    def update_graph_live_terra(n):
        # Collect some data
        data = connect.get_graph_data()
        print('Update graph, data units:', len(data['time']))

        # Create the graph with subplots
        fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.2)
        fig['layout']['margin'] = {
            'l': 30, 'r': 10, 'b': 30, 't': 10
        }
        fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

        fig.append_trace({
            'x': data['time'],
            'y': data['Altitude'],
            'name': 'Altitude',
            'mode': 'lines+markers',
            'type': 'scatter'
        }, 1, 1)
        fig.append_trace({
            'x': data['Longitude'],
            'y': data['Latitude'],
            'text': data['time'],
            'name': 'Longitude vs Latitude',
            'mode': 'lines+markers',
            'type': 'scatter'
        }, 2, 1)

        return fig

create_graphs('terra_topic', 'terra-text', 'terra-graph')
create_graphs('aqua_topic', 'aqua-text', 'aqua-graph')
create_graphs('aura_topic', 'aura-text', 'aura-graph')


if __name__ == '__main__':
    app.run_server(
        host='0.0.0.0',
        port=8050,
        debug=True)