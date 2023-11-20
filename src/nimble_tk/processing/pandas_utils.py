import pandas as pd
import common


def df_to_map(self, key_col, value_col):
    return self[[key_col, value_col]].set_index(key_col).to_dict()[value_col]


pd.DataFrame.to_map = df_to_map


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
pd.Series.vc = pd.Series.value_counts


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


def mcut(series, bins=None, label_range=True, incr=None, minVal=0, maxVal=None,
         retbins=False, continuous_input=True, include_bin_id=True, debug=False, sort_index=False):
    labels = []
    if not bins:
        if minVal == 'min':
            minVal = series.min()
        if not maxVal:
            maxVal = series.max()
        if not incr:
            incr = (maxVal - minVal) / 10
        bins = []
        currVal = minVal
        while currVal < maxVal:
            bins.append(currVal)
            currVal += incr
    bins = bins + [common.ONE_QUADRILLION]  # just a big number for final bin
    for bin_id, bin_value in enumerate(bins):
        if include_bin_id:
            bin_id_str = "%02d. " % bin_id
        else:
            bin_id_str = ''
        if bin_id == len(bins) - 1:
            labels.append('%s>= %s' % (bin_id_str, bins[bin_id - 1]))
        elif bin_id > 0:
            if label_range:
                if continuous_input:
                    # label boundaries are continuous, right exclusive
                    labels.append('%s%s - %s' %
                                  (bin_id_str, bins[bin_id - 1], bins[bin_id]))
                else:
                    # label boundaries are discrete, right inclusive
                    labels.append('%s%s - %s' % (bin_id_str,
                                  bins[bin_id - 1], bins[bin_id] - 1))
            else:
                labels.append('%s' % bins[bin_id - 1])
    ret_val = pd.cut(series, bins=bins, include_lowest=True,
                     right=False, labels=labels).astype(str)

    # not sure if this sorting is required ?????
    if sort_index:
        ret_val.sort_index(inplace=True)

    if debug:
        pd.log_info(f"bins: {bins}, labels: {labels}")

    if sort_index:
        if retbins:
            return pd.Series(ret_val.values), bins
        return pd.Series(ret_val.values)
    else:
        if retbins:
            return ret_val, bins
        return ret_val


def mcut_series(self, bins=None, label_range=True, incr=None, minVal=0, maxVal=None,
                retbins=False, continuous_input=True, include_bin_id=True, debug=False, sort_index=False):
    return mcut(self, bins=bins, label_range=label_range, incr=incr, minVal=minVal, maxVal=maxVal,
                retbins=retbins, continuous_input=continuous_input, include_bin_id=include_bin_id, debug=debug, sort_index=sort_index)


pd.Series.mcut = mcut_series


def remove_tz_info(self):
    c_series = self.copy(deep=True)
    c_series.index = c_series
    return c_series.tz_localize(None).values


pd.Series.remove_tz_info = remove_tz_info
