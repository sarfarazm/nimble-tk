from pandas import DataFrame
from IPython.display import display, HTML
import pandas as pd
import re


def html(html_str:str, tag:str="", style:str="", bold:bool=False, 
         italic:bool=False, font_size:str='') -> None:
    """Utility function to display ad-hoc html content in a jupyter notebook

    Args:
        html_str (str): The html to display.
        tag (str, optional): Enclose the html in this tag.
        style (str, optional): Style attribute of the html tag.
        bold (bool, optional): Whether to display the text in bold.
        italic (bool, optional): Whether to display the text in italics.
        font_size (str, optional): Font size with units e.g. '16px'. Defaults to ''.
    """
    if bold:
        style += 'font-weight:bold;'
    if italic:
        style += 'font-style:italic;'
    if font_size:
        style += f'font-size:{font_size}px;'
    if not tag and style:
        tag = 'span'
    if tag:
        display(HTML("<" + tag + " style='" + style +
                "'" + ">" + html_str + "</" + tag + ">"))
    else:
        display(HTML(html_str))


def h1(html_str):
    html('<h1>%s</h1>' % html_str)


def h2(html_str):
    html('<h2>%s</h2>' % html_str)


def h3(html_str):
    html('<h3>%s</h3>' % html_str)


def h4(html_str):
    html('<h4>%s</h4>' % html_str)


def h5(html_str):
    html('<h4>%s</h4>' % html_str)


def bold(html_str, breakline='<br/>'):
    html(f'<b>{html_str}</b>{breakline}')


def breakline():
    html('<br/>')


special_char_regex = re.compile('\W+')


def df_display(self, **kwargs):
    """
        Utility function to display a dataframe in between the cell execution 
        and not just at the end. 
    """
    if 'float_format' in kwargs:
        float_format = kwargs['float_format']
    else:
        float_format = '%.3f'

    html(self.to_html(**kwargs))


DataFrame.display = df_display

# -------------------------------------------------------------

# import warnings
# warnings.filterwarnings('ignore')

pd.options.display.max_rows = 999
pd.options.display.max_columns = 50
pd.options.display.float_format = '{:,.3f}'.format

# display(HTML(("<style>.container { width:100% !important; }</style>")))
