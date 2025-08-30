from pandas import DataFrame
from IPython.display import display, HTML, Image
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


def html_kv(key, value):
    html(f'<b>{key}:</b> {value}')


special_char_regex = re.compile('\W+')


def df_display(self, border=False, center_header=False, pct_cols=[], pct_precision=2,
                cell_styles={}, index_styles={}, header_styles={}, 
                int_cols = [], float_format='{:,.3f}', 
                index=True, return_html=False):
    """
        Utility function to display a dataframe in between the cell execution 
        and not just at the end. 
    """

    style = self.style

    if border:
        border_val = '1px solid'
        if border and border is not True:
            border_val = border

        style.set_table_styles([
            {'selector': 'th', 'props': f'border: {border_val};'},
            {'selector': 'td', 'props': f'border: {border_val};'}
        ], overwrite=False, axis=0)

    if center_header:
        style.set_table_styles([
            {'selector': 'th', 'props': 'text-align:center'},
        ], overwrite=False, axis=0)

    def int_formatter(num):
        if pd.isnull(num):
            return ''
        return '{:,.0f}'.format(num)

    def float_formatter(num):
        if pd.isnull(num):
            return ''
        return float_format.format(num)
    
    def perc_formatter(num):
        if pd.isnull(num):
            return ''
        if pct_precision == 0:
            formatter = '{:,.0%}'
        elif pct_precision == 1:
            formatter = '{:,.1%}'
        else:
            formatter = '{:,.2%}'
        return formatter.format(num)
    
    for col in self.columns:
        idx = pd.IndexSlice
        slice_ = idx[idx[:], idx[col]]
        if col in int_cols:
            style.format(int_formatter, subset=slice_)
        elif pd.api.types.is_float_dtype(self[col]):
            if col in pct_cols:
                continue
            else:
                style.format(float_formatter, subset=slice_)

    if pct_cols:
        for col in pct_cols:
            idx = pd.IndexSlice
            slice_ = idx[idx[:], idx[col]]
            style.format(perc_formatter, subset=slice_)

    if index_styles:
        for col, (style_key, style_value) in index_styles.items():
            style.apply_index(col, f"{style_key}:{style_value};", axis=0)
    if header_styles:
        for col, (style_key, style_value) in header_styles.items():
            style.apply_index(col, f"{style_key}:{style_value};", axis=1)

    if cell_styles:
        for (row, col), (style_key, style_value) in cell_styles.items():
            idx = pd.IndexSlice
            slice_ = idx[idx[row], idx[col]]
            style.set_properties(**{style_key: style_value}, subset=slice_)

    if not index:
        try:
            style.hide(axis='index')
        except Exception:
            style.hide_index()

    html_str = style.to_html()

    if return_html:
        return html_str

    html(html_str)

# monkey patching the display function to pd.DataFrame
pd.DataFrame.show = df_display


def display_image(image_path, width=None, height=None):
    """Utility function to display an image in a jupyter notebook

    Args:
        image_path (str): Path to the image file.
        width (int, optional): Width of the image in pixels. Defaults to None.
        height (int, optional): Height of the image in pixels. Defaults to None.
    """

    if image_path.startswith('http'):
        display(HTML(f'<img src="{image_path}" ' + (f'width="{width}" ' if width else '') + (f'height="{height}" ' if height else '') + '/>'))

    if width and height:
        display(Image(filename=image_path, width=width, height=height))
    elif width:
        display(Image(filename=image_path, width=width))
    elif height:
        display(Image(filename=image_path, height=height))
    else:
        display(Image(filename=image_path))

# -------------------------------------------------------------

# import warnings
# warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 999)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.float_format', '{:,.3f}'.format)


# display(HTML(("<style>.container { width:100% !important; }</style>")))
