from flask import Flask
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

from mongoDB.Connection import DatabaseConnection
from property_access.accessProperties import accessProperties

con = DatabaseConnection()
config = accessProperties()

training = con.getCollection(config.getMongoDBProperty('TRAINING_GOOD_RAW_DATA'))
prediction = con.getCollection(config.getMongoDBProperty('PREDICTION_OUTPUT'))

trainingdf = pd.DataFrame(list(training.find()))
predictiondf = pd.DataFrame(list(prediction.find()))

trainingdf.drop(['_id'], axis=1, inplace=True)
predictiondf.drop(['_id'], axis=1, inplace=True)

group_By_Wafer = trainingdf[['Wafer','Output']].groupby('Output').count()
fig = px.pie(group_By_Wafer, values='Wafer', names=['Good','Bad'])

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__,external_stylesheets=external_stylesheets)
app.title = "Wafer Detection"
server=app.server

def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])

app.layout = html.Div(children=
                      [
                          html.H2(children='Wafer Detection'),
                          html.Div([
                              html.H4(children='Choose the data range'),
                              dcc.Dropdown(
                                  id="rangeDropdown",
                                  options=[
                                      {'label': '0 to 100', 'value': 100},
                                      {'label': '100 to 200', 'value': 200},
                                      {'label': '200 to 300', 'value': 300},
                                      {'label': '300 to 400', 'value': 400},
                                      {'label': '400 to 500', 'value': 500},
                                      {'label': '500 to 600', 'value': 600},
                                      {'label': '600 to 700', 'value': 700},
                                      {'label': '700 to 800', 'value': 800}
                                  ],
                                  value=100,
                                  multi=False
                              )
                          ]),
                          html.Div([
                            generate_table(trainingdf[['Wafer','Output']])
                              ],
                          style={'width': '20%', 'display': 'inline-block'}),
                          html.Div([
                            dcc.Graph(
                              id='PieChart',
                              figure=fig
                             )
                          ], style={'width': '20%', 'float': 'right', 'display': 'inline-block'}),
                          html.Div([
                                dcc.Graph(id="barchart")

                          ],style={'width': '60%', 'float': 'right', 'display': 'inline-block'})

                      ])

@app.callback(
    dash.dependencies.Output('barchart', 'figure'),
    [dash.dependencies.Input('rangeDropdown', 'value')])
def update_graph(rangeDropdown):
    if rangeDropdown == 100:
        figure = px.bar(trainingdf.iloc[0:100], x='Wafer', y='Output')
    elif rangeDropdown == 200:
        figure = px.bar(trainingdf.iloc[101:200], x='Wafer', y='Output')
    elif rangeDropdown == 300:
        figure = px.bar(trainingdf.iloc[201:300], x='Wafer', y='Output')
    elif rangeDropdown == 400:
        figure = px.bar(trainingdf.iloc[301:400], x='Wafer', y='Output')
    elif rangeDropdown == 500:
        figure = px.bar(trainingdf.iloc[401:500], x='Wafer', y='Output')
    elif rangeDropdown == 600:
        figure = px.bar(trainingdf.iloc[501:600], x='Wafer', y='Output')
    elif rangeDropdown == 700:
        figure = px.bar(trainingdf.iloc[601:700], x='Wafer', y='Output')
    elif rangeDropdown == 800:
        figure = px.bar(trainingdf.iloc[701:800], x='Wafer', y='Output')
    return figure


if __name__ == '__main__':
    app.run_server(debug=True)
