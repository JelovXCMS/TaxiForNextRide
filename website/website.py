import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

from datetime import datetime as dt
from dash.dependencies import Input, Output
import dash_table
from sqlalchemy import create_engine

#df = pd.read_csv('https://gist.githubusercontent.com/chriddyp/c78bf172206ce24f77d6363a2d754b59/raw/c353e8ef842413cae56ae3920b8fd78468aa4cb2/usa-agricultural-exports-2011.csv')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

user='jelov'
password='jelov_psql_pwd'
host='10.0.0.10'
port='5432'
db='db_psql_taxi'
url='postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,db)

con=create_engine(url)
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

q="select cast(AVG(waitt)/60  as decimal(6,2) ) as avg_wait_time ,zoneid from taxi_table where dat between '2019-06-02 12:00:00' and '2019-06-02 13:00:00 'group by zoneid order by Avg_Wait_Time"
#q="select AVG(waitt)/60 as avg_wait_time ,zoneid from taxi_table where dat between '2019-06-02 12:00:00' and '2019-06-02 13:00:00 'group by zoneid order by Avg_Wait_Time"
df=pd.read_sql(q,con)
#df=pd.read_sql("""SELECT * from taxi_table""",con)
#df=pd.read_sql("""SELECT waitt,zoneid from taxi_table""",con)

#select * from taxi_table where dat between '2019-06-02 12:00:00' and '2019-06-02 13:00:00 ';
#select AVG(waitt)/60 as avg_wait_time ,zoneid from taxi_table where dat between '2019-06-02 12:00:00' and '2019-06-02 13:00:00 'group by zoneid order by Avg_Wait_Time

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

dt_taxi = dash_table.DataTable(
    id='datatable_taxi',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
    },
    style_cell={
        'minWidth':'0px','maxWidth':'10px'    
    },
    columns=[{"name":i, "id":i} for i in df.columns],
    data=df.to_dict('records')
)


app.layout = html.Div(children=[
    html.H4(children='NYC Taxi Waiting time : '),

    html.Div(children='''Selct Date and Time '''),

    dcc.DatePickerSingle(
        id='my-date-picker-single',
        min_date_allowed=dt(2019, 6, 1),
        max_date_allowed=dt(2019, 6, 30),
        initial_visible_month=dt(2019, 6, 1),
        date=str(dt(2019, 6, 1, 23, 59, 59))
        ),

    dcc.Input(
        id='timeid',
        placeholder='Enter a time,ex 13:15:01',
        type='text',
        ),  

    html.Div(id='output-container-date-picker-single'),

    dt_taxi,
    #generate_table(df)
])

@app.callback(
    Output('datatable_taxi', 'data'),
    [Input('my-date-picker-single', 'date'), Input('timeid','value'), ],
)
def update_table(date,time):
    date_str=dt.strptime(date.split(' ')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
    hr_int=int(time.split(':')[0])
    min_str=time.split(':')[1]
    sec_str=time.split(':')[2]
    #q="SELECT * from taxi_table"
    #q="select AVG(waitt)/60 as avg_wait_time ,zoneid from taxi_table where dat between '{} {}:{}:{}' and '{} {}:{}:{} 'group by zoneid order by Avg_Wait_Time".format(date_str,hr_int-2,min_str,sec_str,date_str,hr_int,min_str,sec_str)
    q="select cast(AVG(waitt)/60  as decimal(6,2) ) as avg_wait_time ,zoneid from taxi_table where dat between '{} {}:{}:{}' and '{} {}:{}:{} 'group by zoneid order by Avg_Wait_Time".format(date_str,hr_int-2,min_str,sec_str,date_str,hr_int,min_str,sec_str)
    df=pd.read_sql(q,con)
    #df=pd.read_sql("""SELECT * from taxi_table""",con)
    return df.to_dict('records')


#select AVG(waitt)/60 as avg_wait_time ,zoneid from taxi_table where dat between '2019-06-02 12:00:00' and '2019-06-02 13:00:00 'group by zoneid order by Avg_Wait_Time


@app.callback(
    Output('output-container-date-picker-single', 'children'),
    #[Input('my-date-picker-single', 'date')],
    [Input('my-date-picker-single', 'date'), 
    Input('timeid','value'),
        ],
)
#def update_output(date,time):
def update_output(date,time):
    string_prefix = 'You have selected: '
    if date is not None:
        date = dt.strptime(date.split(' ')[0], '%Y-%m-%d')
        date_string = date.strftime('%Y-%m-%d')
        #date_string = date.strftime('%B %d, %Y')
        #return string_prefix + date_string 
        #return string_prefix + date_string +" {}".format(timeid)
        hr_string = time.split(':')[0]
        hr_int=int(hr_string)
        hr_int=int(time.split(':')[0])
        min_str=time.split(':')[1]
        sec_str=time.split(':')[2]
        return date_string +" {}:{}:{}".format(hr_int-1,min_str,sec_str)


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
