import dash
from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
from dashboard import app
import os
import boto3
import base64
import sqlalchemy
import pandas as pd
import numpy as np


def postgres_url():
    '''
    Return url to access the PostgreSQL database
    '''
    return 'postgresql://{}:{}@{}:{}/{}'.format(os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_HOST_PUBLIC'], os.environ['POSTGRES_PORT'], os.environ['POSTGRES_DATABASE'])


def encode(file_path, bucket_name='femto-data'):
    '''
    Encode an image in S3 bucket
    '''
    s3 = boto3.client('s3', region_name='us-west-2')
    file_obj = s3.get_object(Bucket='femto-data', Key=file_path)

    img = file_obj['Body'].read()

    return base64.b64encode(img).decode('utf-8')


def img_tag(df):
    '''
    Return a list of Img objects
    '''
    lst = [ html.Img(src='data:image/jpeg;charset=utf-8;base64,{}'.format(base), alt=path, style={'width': '24%', 'padding': '5px'}, title=path) for path, base in df.itertuples(index=False) ]

    return lst


def show_results(dir_name, engine):
    '''
    Show images that are exact matches and / or potential matches
    '''
    res = engine.execute('SELECT COUNT(*) FROM images_checksum_{};'.format(dir_name))
    num_images = res.fetchone()[0]
    tags = [ html.P('Scanned {} images'.format(num_images),
                style={'font-size':'1.3em'}
            )]

    # Find exact match using checksum data
    df_paths_checksum = pd.read_sql_query('SELECT path FROM images_checksum_{} WHERE content_id IN (SELECT content_id FROM images_checksum_{} GROUP BY content_id HAVING COUNT(*) > 1)'.format(dir_name, dir_name), con = engine)
    df_contents_checksum = pd.read_sql_query('SELECT content_id FROM images_checksum_{} GROUP BY content_id HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC'.format(dir_name), con = engine)
    
    tags += [ html.H3('Exact Match')  ]
    for content_id in df_contents_checksum['content_id']:
        df_images = pd.read_sql_query('SELECT path FROM images_checksum_{} WHERE content_id = {}'.format(dir_name, content_id), con = engine)
        
        encoded_images = [encode(path) for path in df_images['path']]
        df_images['base'] = encoded_images
        
        tags += [ html.Div(img_tag(df_images)) ]
    
    if len(tags) == 2:
        tags += [ html.P('None')  ]

    # Find potential match using base_small data
    cnt_potential_match = 0
    tags += [ html.Hr() ]
    tags += [ html.H3('Potential Match')  ]
    df_contents_base_small = pd.read_sql_query('SELECT content_id FROM images_base_small_{} GROUP BY content_id HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC'.format(dir_name), con = engine)
    
    for content_id in df_contents_base_small['content_id']:
        df_images = pd.read_sql_query('SELECT path FROM images_base_small_{} WHERE content_id = {}'.format(dir_name, content_id), con = engine)

        encoded_images = [ encode(path) for path in df_images['path'] if np.in1d(path, df_paths_checksum['path'], invert=True)[0] ]

        encoded_images = []
        paths = []
        for path in df_images['path']:
            if np.in1d(path, df_paths_checksum['path'])[0] == False:
                encoded_images += [ encode(path) ]
                paths += [ path ]
                cnt_potential_match += 1

        df_maybe = pd.DataFrame(data={'path':paths, 'base':encoded_images})
        
        tags += [ html.Div(img_tag(df_maybe)) ]
    
    if cnt_potential_match == 0:
        tags += [ html.P('None')  ]

    return tags


def custom_tab_label(dir_name, engine):
    '''
    Returns a custom label for Tab 1
    '''
    res = engine.execute('SELECT COUNT(*) FROM images_checksum_{};'.format(dir_name))
    num_images = res.fetchone()[0]
    
    return 'Results ({} images scanned)'.format(num_images)


def get_benchmark_table(engine):
    '''
    Return benchmark table as a DataFrame
    '''
    return pd.read_sql_query('SELECT b.benchmark_id, m.method, d.directory, v.version, b.num_images, b.bytes, b.seconds, b.timestamp FROM benchmarks AS b INNER JOIN methods AS m ON b.method_id = m.method_id INNER JOIN directories AS d ON b.directory_id = d.directory_id INNER JOIN versions AS v ON b.version_id = v.version_id ORDER BY b.timestamp DESC;' , con = engine)


def generate_table(dataframe, max_rows=100):
    '''
    Return DataFrame as a HTML table
    '''
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


engine = sqlalchemy.create_engine(postgres_url())

app.layout = html.Div([
    html.H1('DuplicateDetector demo'),
    
    dcc.Tabs(id="tabs-example", value='tab-1-example', children=[
        
        dcc.Tab(label="Results", value='tab-1-example', children=[
            html.Div(dcc.Input(id='input-box', type='text', value='test_1'), style={'float':'left'}),
            html.Button('See result', id='button'),
            html.Div(id='output-container-button',
                children='Enter a directory name and click See Result')
        ]),
        
        dcc.Tab(label='Benchmark', value='tab-2-example'),
    ]),
    html.Div(id='tabs-content-example')
])


@app.callback(Output('tabs-content-example', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    if tab == 'tab-1-example':
        return html.Div([
            # Intentionally left blank
            # Will be updated by the button callback
        ])
    elif tab == 'tab-2-example':
        df_benchmark = pd.read_sql_query('SELECT m.method, b.num_images, (AVG(b.bytes)/1024)/1024 AS MB, AVG(b.seconds)/60 AS min FROM benchmarks AS b INNER JOIN methods AS m ON b.method_id = m.method_id GROUP BY m.method_id, b.num_images;', con = engine)
        return html.Div([
            html.Div(
                dcc.Graph(
                    id='graph-2-tabs',
                    figure={
                        'data': [{
                            'x': df_benchmark[df_benchmark['method']==i]['num_images'],
                            'y': df_benchmark[df_benchmark['method']==i]['mb'],
                            'name': i,
                            'opacity': 0.75,
                            'marker':{
                                'size': 10
                            },
                            'type': 'scatter'
                        } for i in df_benchmark['method'].unique()],
                        
                        'layout':{
                            'showlegend':False,
                            'title':'Size (MB)',
                            'titlefont':{
                                'size':24
                            },
                            'xaxis':{
                                'title':'Number of images',
                                'titlefont':{
                                    'size':18
                                },
                                'tickfont':{
                                    'size':18
                                }
                            },
                            'yaxis':{
                                #'title':'Size (MB)',
                                'titlefont':{
                                    'size':18
                                },
                                'tickfont':{
                                    'size':18
                                }
                            },
                            'legend':{
                                'font':{
                                    'size':18
                                }
                            },
                            'width':600
                        }
                    }
                ), style={'float':'left'}
            ),

            html.Div(
                dcc.Graph(
                    id='graph-2-tabs',
                    figure={
                        'data': [{
                            'x': df_benchmark[df_benchmark['method']==i]['num_images'],
                            'y': df_benchmark[df_benchmark['method']==i]['min'],
                            'name': i,
                            'opacity': 0.75,
                            'marker':{
                                'size': 10
                            },
                            'type': 'scatter'
                        } for i in df_benchmark['method'].unique()],
                        
                        'layout':{
                            'title':'Time (min)',
                            'titlefont':{
                                'size':24
                            },
                            'xaxis':{
                                'title':'Number of images',
                                'titlefont':{
                                    'size':18
                                },
                                'tickfont':{
                                    'size':18
                                }
                            },
                            'yaxis':{
                                #'title':'Time (min)',
                                'titlefont':{
                                    'size':18
                                },
                                'tickfont':{
                                    'size':18
                                }
                            },
                            'legend':{
                                'font':{
                                    'size':18
                                }
                            },
                            'width':700
                        }
                    }
                ), style={'float':'left'}
            ),
            
            html.Div([
                html.Br(),
                html.Hr(),
                html.H3('Raw benchmark'),
                generate_table(get_benchmark_table(engine)),
            ], style={'clear':'both'})
        ])


@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(n_clicks, value):
    '''
    Show results based on the value entered by the user
    '''
    if value == 'full_data':
        value = 'train'
    res = engine.execute("SELECT COUNT(*) FROM directories WHERE directory='{}';".format(value))
    exists = res.fetchone()[0]

    if exists:
        return show_results(value, engine)

    return 'Result does not exist. Try another directory'
