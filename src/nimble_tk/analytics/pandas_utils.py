import pandas as pd
from nimble_tk import common



def value_counts_perc(self, fill='NA'):
    if fill:
        self.fillna(fill, inplace=True)

    value_count = self.value_counts()
    value_percent = self.value_counts(normalize=True)
    df_ = pd.merge(value_count, value_percent,
                   left_index=True, right_index=True)
    df_.columns = ['COUNT', 'PERC']

    return df_


pd.Series.value_counts_perc = value_counts_perc
pd.Series.vcp = value_counts_perc


# break_in_chunks, break_in_parts
def split(self, n_parts=None, n_rows_per_split=None, ratio=None):
    if n_parts:
        n_rows_per_split = int(self.shape[0] / n_parts)
    elif ratio:
        n_rows_per_split = int(self.shape[0]*ratio)
    for start in range(0, self.shape[0], n_rows_per_split):
        df_split = self.iloc[start:start + n_rows_per_split]
        yield df_split


pd.DataFrame.split = split


def flattened_columns(self, separator='_'):
    """
    flatten or collapse multi-level columns
    """
    cols = []
    for col in self.columns.values:
        # print(col)
        if type(col) == str:
            cols.append(col)
        else:
            col = [col1 for col1 in col if col1]
            cols.append(separator.join(col).strip())
    df_copy = self.copy()
    df_copy.columns = cols
    return df_copy


pd.DataFrame.flattened_columns = flattened_columns


def df_reverse_sort_values(self, *args, **kwargs):
    kwargs['ascending'] = False
    return self.sort_values(*args, **kwargs)


pd.DataFrame.rsort = df_reverse_sort_values
pd.DataFrame.sort = pd.DataFrame.sort_values


def sr_reverse_sort_values(self, *args, **kwargs):
    kwargs['ascending'] = False
    return self.sort_values(*args, **kwargs)


pd.Series.rsort = sr_reverse_sort_values


def mcut(series:pd.Series, bins:list=None, minVal:int=0, maxVal:int=None, 
         step_size:float=None, continuous_input:bool=True, 
         include_bin_id:bool=False) -> pd.Series:
    """A utility wrapper around the pandas `series.cut` function. 
        Creates the label values based on the provided inputs.
        
    Args:
        series (pd.Series): The input pandas series
        bins (list, optional): A list of numbers which form the boundaries of
            bins. If the bins list is not given, it is computed based on
            the `minVal`, `maxVal` and `step_size` parameters.
        minVal (int, optional): Min value of the first bin. Defaults to 0.
            This is used only in case where the `bins` parameter is not given.
        maxVal (int, optional): Max value of the last bin.
            This is used only in case where the `bins` parameter is not given.
        step_size (float, optional): _description_. 
            Defaults to `(maxVal - minVal) / 10`.
            This is used only in case where the `bins` parameter is not given.
        continuous_input (bool, optional): Whether the input values are 
            continuous or discrete. Defaults to True.
        include_bin_id (bool, optional): _description_. Defaults to False.

    Returns:
        pd.Series: _description_
    """
    labels = []
    if not bins:
        if not minVal:
            minVal = series.min()
        if not maxVal:
            maxVal = series.max()
        if not step_size:
            step_size = (maxVal - minVal) / 10
        bins = []
        currVal = minVal
        while currVal < maxVal:
            bins.append(currVal)
            currVal += step_size
    bins = bins + [common.ONE_QUADRILLION]  # just a big number for final bin
    for bin_id, _ in enumerate(bins):
        if include_bin_id:
            bin_id_str = "%02d. " % bin_id
        else:
            bin_id_str = ''
        if bin_id == len(bins) - 1:
            labels.append('%s>= %s' % (bin_id_str, bins[bin_id - 1]))
        elif bin_id > 0:
            if continuous_input:
                # label boundaries are continuous, right exclusive
                labels.append('%s%s - %s' %
                                (bin_id_str, bins[bin_id - 1], bins[bin_id]))
            else:
                # label boundaries are discrete, right inclusive
                labels.append('%s%s - %s' % (bin_id_str,
                                bins[bin_id - 1], bins[bin_id] - 1))
    ret_val = pd.cut(series, bins=bins, include_lowest=True,
                     right=False, labels=labels).astype(str)

    return ret_val


pd.Series.mcut = mcut


def write_dfs_to_excel(dfs_map:dict[str, pd.DataFrame], file_name:str, 
                       percent_cols:list=[], text_cols:list=[], 
                       index:list=False) -> None:
    """_summary_

    Args:
        dfs_map (dict[str, pd.DataFrame]): _description_
        file_name (str): _description_
        percent_cols (list, optional): _description_. Defaults to [].
        text_cols (list, optional): _description_. Defaults to [].
        index (list, optional): _description_. Defaults to False.
    """
    common.log_info(f"Write to excel - {file_name}")
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter',
                            datetime_format='mmm d yyyy hh:mm:ss', date_format='mmm dd yyyy')
    workbook = writer.book
    num_format = workbook.add_format(
        {'num_format': '[>9999999]##\,##\,##\,##0; [>99999]##\,##\,##0; ##,##0'})
    percent_fmt = workbook.add_format({'num_format': '0.00%'})
    text_format = workbook.add_format({'num_format': '@'})
    for sheet, df in dfs_map.items():
        common.log_info(f"Write to excel - writing sheet - {sheet}")
        df.to_excel(writer, sheet, index=index)

    for sheet, df in dfs_map.items():

        df_sample = df.sample(
            n=min(df.shape[0], 100)).reset_index(drop=(not index))

        for column in df_sample.columns:
            col_format = None
            max_length_for_col = df_sample[column].astype(str).map(len).max()
            # if column name is bigger than column content
            max_length_for_col = max(max_length_for_col, len(column))
            # limit column size to 250 chars
            max_length_for_col = min(max_length_for_col, 250)
            column_length = max_length_for_col
            if column_length > 150:
                column_length = 150
            col_idx = df_sample.columns.get_loc(column)

            if column in text_cols:
                col_format = text_format
            elif 'int' in str(df_sample[column].dtype) or 'float' in str(df_sample[column].dtype):
                if column in percent_cols:
                    col_format = percent_fmt
                else:
                    col_format = num_format
            elif 'datetime64[ns' in str(df_sample[column].dtype):
                column_length = 19
            else:
                # writer.sheets[sheet].set_column(col_idx, col_idx, column_length + 2)
                pass

            writer.sheets[sheet].set_column(
                col_idx, col_idx, column_length + 2, col_format)

    try:
        writer.save()
    except:
        writer.close()

    common.log_info("Write to excel - done")


def df_to_map(self, key_col, value_col):
    return self[[key_col, value_col]].set_index(key_col).to_dict()[value_col]


pd.DataFrame.to_map = df_to_map


def remove_tz_info(self):
    c_series = self.copy(deep=True)
    c_series.index = c_series
    return c_series.tz_localize(None).values


pd.Series.remove_tz_info = remove_tz_info
