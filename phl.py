import pandas as pd
import numpy as np

def previous_high_low(ohlc, time_frame: str = "1D"):
        """
        Previous High Low
        This method returns the previous high and low of the given time frame.

        parameters:
        time_frame: str - the time frame to get the previous high and low 15m, 1H, 4H, 1D, 1W, 1M

        returns:
        PreviousHigh = the previous high
        PreviousLow = the previous low
        """

        ohlc = ohlc.copy()
        df_m = ohlc.copy()

        ohlc.index = pd.to_datetime(ohlc.time)

        previous_high = np.zeros(len(ohlc), dtype=np.float32)
        previous_low = np.zeros(len(ohlc), dtype=np.float32)
        broken_high = np.zeros(len(ohlc), dtype=np.int32)
        broken_low = np.zeros(len(ohlc), dtype=np.int32)

        ''' 
        Метод resample повзоляет изменить ТФ данных
        То есть здесь ДФ сводится к указанному ТФ, с помощью агрегации
        Остальное выбрасывается
        Так на входе может быть 48 часовых свечей, а получится 2 дневных
        '''
        resampled_ohlc = ohlc.resample(time_frame).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        ).dropna()

        if time_frame == '1D':
            index = -2
        elif time_frame == '1W' or time_frame == '1M':
            index = -1
        else:
            index = -2

        currently_broken_high = False
        currently_broken_low = False
        last_broken_time = None
        for i in range(len(ohlc)):
            resampled_previous_index = np.where(
                resampled_ohlc.index < ohlc.index[i]
            )[0]
            if len(resampled_previous_index) <= 1:
                previous_high[i] = np.nan
                previous_low[i] = np.nan
                continue

            # Тут должно быть значение индекса -1 для Недельно и Месячного ТФ и -2 для Дневного
            resampled_previous_index = resampled_previous_index[index]

            if last_broken_time != resampled_previous_index:
                currently_broken_high = False
                currently_broken_low = False
                last_broken_time = resampled_previous_index

            previous_high[i] = resampled_ohlc["high"].iloc[resampled_previous_index] 
            previous_low[i] = resampled_ohlc["low"].iloc[resampled_previous_index]
            currently_broken_high = ohlc["high"].iloc[i] > previous_high[i] or currently_broken_high
            currently_broken_low = ohlc["low"].iloc[i] < previous_low[i] or currently_broken_low
            broken_high[i] = 1 if currently_broken_high else 0
            broken_low[i] = 1 if currently_broken_low else 0
        
        if time_frame == '1D':
            name = 'd'
        elif time_frame == '1W':
            name = 'w'

        previous_high = pd.Series(previous_high, name=f"p{name}h")
        previous_low = pd.Series(previous_low, name=f"p{name}l")
        broken_high = pd.Series(broken_high, name=f"p{name}h_break")
        broken_low = pd.Series(broken_low, name=f"p{name}l_break")

        previous_high_low_df = pd.concat([previous_high, previous_low, broken_high, broken_low], axis=1)
        df = df_m.merge(previous_high_low_df, left_index=True, right_index=True)

        return df, previous_high_low_df


def session(df):

    # 0 - Asia
    # 1 - LOKZ
    # 2 - NYKZ
    # 3 - PM 

    df = df.copy()

    df['open_time_h'] = pd.to_datetime(df['time']).dt.hour

    conditions = [
        (df['open_time_h'] >= 4) & (df['open_time_h'] < 10),
        (df['open_time_h'] >= 10) & (df['open_time_h'] < 16),
        (df['open_time_h'] >= 16) & (df['open_time_h'] < 22),
        (df['open_time_h'] >= 22) | (df['open_time_h'] < 4)
    ]
    choices = [1, 2, 3, 0]

    df['kz'] = np.select(conditions, choices, default=np.nan)
    df['month'] = pd.to_datetime(df['time']).dt.month

    return df