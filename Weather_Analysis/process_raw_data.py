import glob
from datetime import datetime, time

import pandas as pd


def ProcessRawData():
    path = "RawData"

    def extract_date_time(filename):
        """
        extract the date and time from the filename
        :param filename:
        :return:
        """
        parts = filename.split('.')
        date = parts[1]
        time = parts[2]
        return date, time

    def get_date(df, file):
        """get the date from the dataframe and the time from the filename and combine them into a datetime object
        :param df: dataframe containing the date
        :param file: filename containing the time
        :return: datetime object
        """
        # date_str = df[df.iloc[:, 2] == 1].iloc[0]['Date']
        date_str = str(file.split('.')[1])
        time_str = str(file.split('.')[2])
        # date = datetime.strptime(date_str, '%Y-%m-%d')
        date = datetime.strptime(date_str, '%Y%m%d')
        time_value = time(int(time_str), 0)
        combined_datetime = datetime.combine(date.date(), time_value)
        return combined_datetime

    degree_days = 'gw_hdd'
    ecmwf_files = glob.glob(path + f'/ecmwf.*.[01][02].{degree_days}.csv')
    ecmwf_sorted_files = sorted(ecmwf_files, key=lambda x: (x.split('.')[1], x.split('.')[2]))[3:]

    ecmwf_eps_files = glob.glob(path + f'/ecmwf-eps.*.[01][02].{degree_days}.csv')
    ecmwf_eps_sorted_files = sorted(ecmwf_eps_files, key=lambda x: (x.split('.')[1], x.split('.')[2]))[2:]

    gfs_ens_bc_files = glob.glob(path + f'/gfs-ens-bc.*.[01][02].{degree_days}.csv')
    gfs_ens_bc_sorted_files = sorted(gfs_ens_bc_files, key=lambda x: (x.split('.')[1], x.split('.')[2]))[2:]

    cmc_ens_files = glob.glob(path + f'/cmc-ens.*.[01][02].{degree_days}.csv')
    cmc_ens_sorted_files = sorted(cmc_ens_files, key=lambda x: (x.split('.')[1], x.split('.')[2]))[2:]
    for _ in range(2):
        set1 = set((extract_date_time(filename) for filename in ecmwf_sorted_files))
        set2 = set((extract_date_time(filename) for filename in ecmwf_eps_sorted_files))

        ecmwf_sorted_files = [filename for filename in ecmwf_sorted_files if extract_date_time(filename) in set2]
        ecmwf_eps_sorted_files = [filename for filename in ecmwf_eps_sorted_files if
                                  extract_date_time(filename) in set1]
        cmc_ens_sorted_files = [filename for filename in cmc_ens_sorted_files if extract_date_time(filename) in set1]

        master_set = set((extract_date_time(filename) for filename in cmc_ens_sorted_files))
        gfs_ens_bc_sorted_files = [filename for filename in gfs_ens_bc_sorted_files if
                                   extract_date_time(filename) in master_set]

        master_set = set((extract_date_time(filename) for filename in gfs_ens_bc_sorted_files))

        ecmwf_sorted_files = [filename for filename in ecmwf_sorted_files if extract_date_time(filename) in master_set]
        ecmwf_eps_sorted_files = [filename for filename in ecmwf_eps_sorted_files if
                                  extract_date_time(filename) in master_set]
        gfs_ens_bc_sorted_files = [filename for filename in gfs_ens_bc_sorted_files if
                                   extract_date_time(filename) in master_set]
        cmc_ens_sorted_files = [filename for filename in cmc_ens_sorted_files if
                                extract_date_time(filename) in master_set]

    ecmwf_eps_change_df = pd.DataFrame(columns=['ecmwf-eps_9', 'ecmwf-eps_10', 'ecmwf-eps_11', 'ecmwf-eps_12',
                                                'ecmwf-eps_13', 'ecmwf-eps_14'])
    passed_rows = []

    for i in range(1, len(ecmwf_eps_sorted_files)):
        ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i])
        ecmwf_eps_df = ecmwf_eps_df[ecmwf_eps_df[ecmwf_eps_df.columns[2]] >= 1]
        prev_ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 1])
        prev_ecmwf_eps_df = prev_ecmwf_eps_df[prev_ecmwf_eps_df[prev_ecmwf_eps_df.columns[2]] >= 1]

        date = get_date(ecmwf_eps_df, ecmwf_eps_sorted_files[i])
        prev_date = get_date(prev_ecmwf_eps_df, ecmwf_eps_sorted_files[i - 1])
        d2 = str(date)[:10]
        d1 = str(prev_date)[:10]

        if d2 != d1:
            offset = 1
        else:
            offset = 0

        changes = []
        try:
            for day in range(8, 14):
                changes.append(ecmwf_eps_df.iloc[day - offset]['Value'] - prev_ecmwf_eps_df.iloc[day]['Value'])
            new_row = pd.DataFrame([changes], columns=ecmwf_eps_change_df.columns, index=[date])
            ecmwf_eps_change_df = pd.concat([ecmwf_eps_change_df, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    ecmwf_change_df = pd.DataFrame(columns=['ecmwf_diff_8', 'ecmwf_diff_9', ])

    for i in range(1, len(ecmwf_sorted_files)):
        ecmwf_df = pd.read_csv(ecmwf_sorted_files[i])
        ecmwf_df = ecmwf_df[ecmwf_df[ecmwf_df.columns[2]] >= 1]
        ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 1])
        ecmwf_eps_df = ecmwf_eps_df[ecmwf_eps_df[ecmwf_eps_df.columns[2]] >= 1]

        try:
            ecmwf = ecmwf_df.iloc[8]
            ecmwf_eps = ecmwf_eps_df.iloc[9]
        except IndexError:
            print(f"error on row: {i}")
            passed_rows.append(i)
            continue

        date = get_date(ecmwf_df, ecmwf_sorted_files[i])
        prev_date = get_date(ecmwf_eps_df, ecmwf_eps_sorted_files[i - 1])
        d2 = str(date)[:10]
        d1 = str(prev_date)[:10]
        if d2 != d1:
            offset = 1
        else:
            offset = 0

        changes = []
        try:
            for day in range(8, 10):
                changes.append(ecmwf_df.iloc[day - offset]['Value'] - ecmwf_eps_df.iloc[day]['Value'])
            new_row = pd.DataFrame([changes], columns=ecmwf_change_df.columns, index=[date])
            ecmwf_change_df = pd.concat([ecmwf_change_df, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    gfs_ens_bc_change_df = pd.DataFrame(columns=['gfs-ens-bc_9', 'gfs-ens-bc_10', 'gfs-ens-bc_11', 'gfs-ens-bc_12',
                                                 'gfs-ens-bc_13', 'gfs-ens-bc_14'])

    for i in range(1, len(gfs_ens_bc_sorted_files)):
        gfs_ens_bc_df = pd.read_csv(gfs_ens_bc_sorted_files[i])
        gfs_ens_bc_df = gfs_ens_bc_df[gfs_ens_bc_df[gfs_ens_bc_df.columns[2]] >= 1]
        prev_ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 1])
        prev_ecmwf_eps_df = prev_ecmwf_eps_df[prev_ecmwf_eps_df[prev_ecmwf_eps_df.columns[2]] >= 1]

        try:
            date = get_date(gfs_ens_bc_df, gfs_ens_bc_sorted_files[i])
            prev_date = get_date(prev_ecmwf_eps_df, ecmwf_eps_sorted_files[i - 1])
        except IndexError:
            print(f"error on row: {i}")
            passed_rows.append(i)
            continue

        d2 = str(date)[:10]
        d1 = str(prev_date)[:10]
        if d2 != d1:
            offset = 1
        else:
            offset = 0

        changes = []
        try:
            for day in range(8, 14):
                changes.append(gfs_ens_bc_df.iloc[day - offset]['Value'] - prev_ecmwf_eps_df.iloc[day]['Value'])
            new_row = pd.DataFrame([changes], columns=gfs_ens_bc_change_df.columns, index=[date])
            gfs_ens_bc_change_df = pd.concat([gfs_ens_bc_change_df, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    cmc_ens_change_df = pd.DataFrame(columns=['cmc-ens_9', 'cmc-ens_10', 'cmc-ens_11', 'cmc-ens_12',
                                              'cmc-ens_13', 'cmc-ens_14'])

    for i in range(1, len(cmc_ens_sorted_files)):
        cmc_ens_df = pd.read_csv(cmc_ens_sorted_files[i])
        cmc_ens_df = cmc_ens_df[cmc_ens_df[cmc_ens_df.columns[2]] >= 1]
        gfs_ens_bc_df = pd.read_csv(gfs_ens_bc_sorted_files[i])
        gfs_ens_bc_df = gfs_ens_bc_df[gfs_ens_bc_df[gfs_ens_bc_df.columns[2]] >= 1]
        date = get_date(cmc_ens_df, cmc_ens_sorted_files[i])

        changes = []
        try:
            for day in range(8, 14):
                changes.append(cmc_ens_df.iloc[day]['Value'] - gfs_ens_bc_df.iloc[day]['Value'])
            new_row = pd.DataFrame([changes], columns=cmc_ens_change_df.columns, index=[date])
            cmc_ens_change_df = pd.concat([cmc_ens_change_df, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    day_8_error = pd.DataFrame(columns=['day_8_error'])

    for i in range(1, len(ecmwf_eps_sorted_files)):
        ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i])
        ecmwf_eps_df = ecmwf_eps_df[ecmwf_eps_df[ecmwf_eps_df.columns[2]] >= 1]
        prev_ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 1])
        prev_ecmwf_eps_df = prev_ecmwf_eps_df[prev_ecmwf_eps_df[prev_ecmwf_eps_df.columns[2]] >= 1]

        date = get_date(ecmwf_eps_df, ecmwf_eps_sorted_files[i])
        prev_date = get_date(prev_ecmwf_eps_df, ecmwf_eps_sorted_files[i - 1])
        d2 = str(date)[:10]
        d1 = str(prev_date)[:10]

        if d2 != d1:
            offset = 1
        else:
            offset = 0
        day = 7
        changes = []
        try:
            changes.append(ecmwf_eps_df.iloc[day]['Value'] - prev_ecmwf_eps_df.iloc[day + offset]['Value'])
            new_row = pd.DataFrame([changes], columns=day_8_error.columns, index=[date])
            day_8_error = pd.concat([day_8_error, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    errors_df = pd.DataFrame(columns=['error_9', 'error_10', 'error_11', 'error_12', 'error_13', 'error_14'])

    for i in range(2, len(ecmwf_eps_sorted_files)):
        ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 1])
        ecmwf_eps_df = ecmwf_eps_df[ecmwf_eps_df[ecmwf_eps_df.columns[2]] >= 1]
        prev_ecmwf_eps_df = pd.read_csv(ecmwf_eps_sorted_files[i - 2])
        prev_ecmwf_eps_df = prev_ecmwf_eps_df[prev_ecmwf_eps_df[prev_ecmwf_eps_df.columns[2]] >= 1]

        date = get_date(ecmwf_eps_df, ecmwf_eps_sorted_files[i])
        prev_date = get_date(prev_ecmwf_eps_df, ecmwf_eps_sorted_files[i - 1])
        d2 = str(date)[:10]
        d1 = str(prev_date)[:10]

        if d2 != d1:
            offset = 1
        else:
            offset = 0

        errors = []
        try:
            for day in range(8, 14):
                errors.append(ecmwf_eps_df.iloc[day - offset]['Value'] - prev_ecmwf_eps_df.iloc[day]['Value'])
            new_row = pd.DataFrame([errors], columns=errors_df.columns, index=[date])
            errors_df = pd.concat([errors_df, new_row])
        except IndexError:
            print(f"error on {date}")
            passed_rows.append(i)

    errors_df['noon'] = errors_df.index.hour
    errors_df['noon'] = errors_df['noon'].apply(lambda x: 1 if x == 12 else 0)

    master_df = pd.concat(
        [gfs_ens_bc_change_df, cmc_ens_change_df, ecmwf_change_df, errors_df, day_8_error, ecmwf_eps_change_df], axis=1)
    master_df.fillna(0, inplace=True)

    master_df.to_pickle('master_df.pkl')


if __name__ == '__main__':
    ProcessRawData()
