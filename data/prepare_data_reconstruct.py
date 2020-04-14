import pandas as pd
import numpy

# read CSV
df = pd.read_csv('Street_Construction_Permits.csv', usecols=['IssuedWorkEndDate', 'BoroughName', 'PermitTypeDesc'])

# filter PermitType, and leave in df only connectd with street reconstrction
df = df.loc[(df['PermitTypeDesc'] == 'OCCUPANCY OF SIDEWALK AS STIPULATED') |
            (df['PermitTypeDesc'] == 'OCCUPANCY OF ROADWAY AS STIPULATED') |
            (df['PermitTypeDesc'] == 'REPAIR SIDEWALK') |
            (df['PermitTypeDesc'] == 'CROSSING SIDEWALK') |
            (df['PermitTypeDesc'] == 'TEMPORARY PEDESTRIAN WALK') |
            (df['PermitTypeDesc'] == 'REPLACE SIDEWALK') |
            (df['PermitTypeDesc'] == 'CONSTRUCT NEW SIDEWALK BLG. PAVEMENT') |
            (df['PermitTypeDesc'] == 'SIDEWALK RECONSTRUCTION CONTRACTS') |
            (df['PermitTypeDesc'] == 'REPAIR TRAFFIC STREET LIGHT') |
            (df['PermitTypeDesc'] == 'INSTALL TRAFFIC SIGNALS') |
            (df['PermitTypeDesc'] == 'SIDEWALK RECONSTRUCTION CONTRACTS-PROT') |
            (df['PermitTypeDesc'] == 'OPEN SIDEWALK TO INSTALL FOUNDATION') |
            (df['PermitTypeDesc'] == 'REPAIR TRAFFIC STREET LIGHT - PROTECTED') |
            (df['PermitTypeDesc'] == 'INSTALL TRAFFIC SIGNALS - PROTECTED') |
            (df['PermitTypeDesc'] == 'TRANSFORMER VAULT - IN SIDEWALK AREA') |
            (df['PermitTypeDesc'] == 'OPEN PROTECTED SIDEWALK FOR FOUNDATION') |
            (df['PermitTypeDesc'] == 'CONSTRUCT NEW SIDEWALK') |
            (df['PermitTypeDesc'] == 'TRANSFORMER VAULT-IN SIDEWALK AREA-PROT') |
            (df['PermitTypeDesc'] == 'REPAIR TRAFFIC SIGNALS') |
            (df['PermitTypeDesc'] == 'INSTALL TRAFFIC STREET LIGHTS') |
            (df['PermitTypeDesc'] == 'INSTALL STREET FURNITURE') |
            (df['PermitTypeDesc'] == 'INSTALL TRAFFIC LIGHTS - PROTECTED') |
            (df['PermitTypeDesc'] == 'INSTALL STREET FURNITURE - PROTECTED') |
            (df['PermitTypeDesc'] == 'INSTALL TEMP. DECORATIVE LIGHT ON STREET') |
            (df['PermitTypeDesc'] == 'LEADER DRAIN UNDER SIDEWALK') |
            (df['PermitTypeDesc'] == 'INSTALL BIKE RACK ON SIDEWALK') |
            (df['PermitTypeDesc'] == 'INSTALL TEMPORARY SECURITY STRUCTURE') |
            (df['PermitTypeDesc'] == 'REPLACE/INSTALL STREET LIGHTS-PROTECTED') |
            (df['PermitTypeDesc'] == 'REPLACE/INSTALL STREET LIGHTS')
            ]
# translate to datetime
df['IssuedWorkEndDate'] = df['IssuedWorkEndDate'].apply(lambda x: str(x)[:-6])
df = df[df['IssuedWorkEndDate'] != '']
df['IssuedWorkEndDate'] = pd.to_datetime(df['IssuedWorkEndDate'], format='%m/%d/%Y %I:%M:%S %p')

# split the same Permittype
df.loc[df['PermitTypeDesc'] == 'REPAIR TRAFFIC STREET LIGHT - PROTECTED', [
    'PermitTypeDesc']] = 'REPLACE/INSTALL STREET LIGHTS-PROTECTED'
df.loc[df['PermitTypeDesc'] == 'INSTALL TRAFFIC STREET LIGHTS', [
    'PermitTypeDesc']] = 'REPLACE/INSTALL STREET LIGHTS-PROTECTED'
df.loc[df['PermitTypeDesc'] == 'REPLACE/INSTALL STREET LIGHTS', [
    'PermitTypeDesc']] = 'REPLACE/INSTALL STREET LIGHTS-PROTECTED'
df.loc[df['PermitTypeDesc'] == 'INSTALL TRAFFIC LIGHTS - PROTECTED', [
    'PermitTypeDesc']] = 'REPLACE/INSTALL STREET LIGHTS-PROTECTED'
df.loc[df['PermitTypeDesc'] == 'INSTALL STREET FURNITURE - PROTECTED', ['PermitTypeDesc']] = 'INSTALL STREET FURNITURE'
df.loc[df['PermitTypeDesc'] == 'CONSTRUCT NEW SIDEWALK', ['PermitTypeDesc']] = 'CONSTRUCT NEW SIDEWALK BLG. PAVEMENT'
df.loc[df['PermitTypeDesc'] == 'LEADER DRAIN UNDER SIDEWALK', ['PermitTypeDesc']] = 'SIDEWALK RECONSTRUCTION CONTRACTS'
df.loc[
    df['PermitTypeDesc'] == 'INSTALL BIKE RACK ON SIDEWALK', ['PermitTypeDesc']] = 'SIDEWALK RECONSTRUCTION CONTRACTS'
df.loc[df['PermitTypeDesc'] == 'INSTALL TEMP. DECORATIVE LIGHT ON STREET', [
    'PermitTypeDesc']] = 'REPLACE/INSTALL STREET LIGHTS-PROTECTED'
df.loc[df['PermitTypeDesc'] == 'TRANSFORMER VAULT-IN SIDEWALK AREA-PROT', [
    'PermitTypeDesc']] = 'TRANSFORMER VAULT - IN SIDEWALK AREA'
df.loc[df['PermitTypeDesc'] == 'REPAIR TRAFFIC SIGNALS ', ['PermitTypeDesc']] = 'INSTALL TRAFFIC SIGNALS'
df.loc[df['PermitTypeDesc'] == 'INSTALL TEMPORARY SECURITY STRUCTURE', ['PermitTypeDesc']] = 'TEMPORARY PEDESTRIAN WALK'
df.loc[df['PermitTypeDesc'] == 'INSTALL TRAFFIC SIGNALS - PROTECTED', ['PermitTypeDesc']] = 'INSTALL TRAFFIC SIGNALS'
df.loc[df['PermitTypeDesc'] == 'REPAIR TRAFFIC SIGNALS', ['PermitTypeDesc']] = 'INSTALL TRAFFIC SIGNALS'
df.loc[df['PermitTypeDesc'] == 'OPEN PROTECTED SIDEWALK FOR FOUNDATION', [
    'PermitTypeDesc']] = 'SIDEWALK RECONSTRUCTION CONTRACTS'

df = df[(df['IssuedWorkEndDate'] > '01/01/2013 00:00:00') & (df['IssuedWorkEndDate'] < '01/01/2020 00:00:00')]
# create sample_df with resample by year, and drop unnecessary columns
sample_df = df.groupby(['PermitTypeDesc', 'BoroughName']).resample('Y', on='IssuedWorkEndDate').count()
sample_df.index = sample_df.index.set_names(['PermitType', 'Borough', 'EndDate'])
sample_df = sample_df.reset_index()
sample_df.drop(['IssuedWorkEndDate'], axis=1, inplace=True)
sample_df.drop(['BoroughName'], axis=1, inplace=True)

# create and fill Accidents column
sample_df['Accidents'] = 0
sample_df.loc[(sample_df['EndDate'] == '2013-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 21237
sample_df.loc[(sample_df['EndDate'] == '2014-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 21068
sample_df.loc[(sample_df['EndDate'] == '2015-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 23012
sample_df.loc[(sample_df['EndDate'] == '2016-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 28046
sample_df.loc[(sample_df['EndDate'] == '2017-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 33541
sample_df.loc[(sample_df['EndDate'] == '2018-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 33744
sample_df.loc[(sample_df['EndDate'] == '2019-12-31') & (sample_df['Borough'] == 'BRONX'), ['Accidents']] = 31022

sample_df.loc[(sample_df['EndDate'] == '2013-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 50271
sample_df.loc[(sample_df['EndDate'] == '2014-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 51154
sample_df.loc[(sample_df['EndDate'] == '2015-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 54882
sample_df.loc[(sample_df['EndDate'] == '2016-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 56298
sample_df.loc[(sample_df['EndDate'] == '2017-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 63097
sample_df.loc[(sample_df['EndDate'] == '2018-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 63355
sample_df.loc[(sample_df['EndDate'] == '2019-12-31') & (sample_df['Borough'] == 'BROOKLYN'), ['Accidents']] = 59166

sample_df.loc[(sample_df['EndDate'] == '2013-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 47019
sample_df.loc[(sample_df['EndDate'] == '2014-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 49048
sample_df.loc[(sample_df['EndDate'] == '2015-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 52176
sample_df.loc[(sample_df['EndDate'] == '2016-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 54563
sample_df.loc[(sample_df['EndDate'] == '2017-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 60860
sample_df.loc[(sample_df['EndDate'] == '2018-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 62576
sample_df.loc[(sample_df['EndDate'] == '2019-12-31') & (sample_df['Borough'] == 'QUEENS'), ['Accidents']] = 59210

sample_df.loc[(sample_df['EndDate'] == '2013-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 9354
sample_df.loc[(sample_df['EndDate'] == '2014-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 7742
sample_df.loc[(sample_df['EndDate'] == '2015-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 7582
sample_df.loc[(sample_df['EndDate'] == '2016-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 9885
sample_df.loc[(sample_df['EndDate'] == '2017-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 11608
sample_df.loc[(sample_df['EndDate'] == '2018-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 11629
sample_df.loc[(sample_df['EndDate'] == '2019-12-31') & (sample_df['Borough'] == 'STATEN ISLAND'), ['Accidents']] = 6688

sample_df.loc[(sample_df['EndDate'] == '2013-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 43844
sample_df.loc[(sample_df['EndDate'] == '2014-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 43529
sample_df.loc[(sample_df['EndDate'] == '2015-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 45111
sample_df.loc[(sample_df['EndDate'] == '2016-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 42478
sample_df.loc[(sample_df['EndDate'] == '2017-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 44863
sample_df.loc[(sample_df['EndDate'] == '2018-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 42845
sample_df.loc[(sample_df['EndDate'] == '2019-12-31') & (sample_df['Borough'] == 'MANHATTAN'), ['Accidents']] = 37725

# count correlation
sample_df['Corr'] = 0
sample_df.rename(columns={'PermitTypeDesc': 'reconstruction'}, inplace=True)

permit_type = ['OCCUPANCY OF SIDEWALK AS STIPULATED', 'OCCUPANCY OF ROADWAY AS STIPULATED', 'CROSSING SIDEWALK',
               'REPAIR SIDEWALK', 'TEMPORARY PEDESTRIAN WALK', 'REPLACE SIDEWALK', 'SIDEWALK RECONSTRUCTION CONTRACTS',
               'SIDEWALK RECONSTRUCTION CONTRACTS-PROT', 'INSTALL TRAFFIC SIGNALS',
               'CONSTRUCT NEW SIDEWALK BLG. PAVEMENT',
               'REPAIR TRAFFIC STREET LIGHT', 'REPLACE/INSTALL STREET LIGHTS-PROTECTED',
               'OPEN SIDEWALK TO INSTALL FOUNDATION',
               'TRANSFORMER VAULT - IN SIDEWALK AREA', 'INSTALL STREET FURNITURE']
boroughs = ['BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND', 'MANHATTAN']
for i in permit_type:
    for j in boroughs:
        sample_df.loc[(sample_df['PermitType'] == i) & (sample_df['Borough'] == j), ['Corr']] = \
            numpy.corrcoef(sample_df[(sample_df['PermitType'] == i) & (sample_df['Borough'] == j)]['reconstruction'],
                           sample_df[(sample_df['PermitType'] == i) & (sample_df['Borough'] == j)]['Accidents'])[0][1]


sample_df.drop(['reconstruction','Accidents'], axis=1, inplace=True)
sample_df = sample_df.groupby(['PermitType','Borough']).mean().reset_index()

#create new df for city correlation
city_df = df.groupby(['PermitTypeDesc']).resample('Y', on='IssuedWorkEndDate').count()
city_df.index = city_df.index.set_names(['PermitType', 'EndDate'])
city_df = city_df.reset_index()
city_df.drop(['IssuedWorkEndDate'], axis=1, inplace=True)
city_df['Accidents'] = 0
city_df['Borough'] = 'CITY'

city_df.loc[city_df['EndDate'] == '2013-12-31', ['Accidents']] = 171936
city_df.loc[city_df['EndDate'] == '2014-12-31', ['Accidents']] = 172741
city_df.loc[city_df['EndDate'] == '2015-12-31', ['Accidents']] = 190620
city_df.loc[city_df['EndDate'] == '2016-12-31', ['Accidents']] = 227340
city_df.loc[city_df['EndDate'] == '2017-12-31', ['Accidents']] = 282468
city_df.loc[city_df['EndDate'] == '2018-12-31', ['Accidents']] = 308656
city_df.loc[city_df['EndDate'] == '2019-12-31', ['Accidents']] = 284458

city_df['Corr'] = 0
permit_type = ['OCCUPANCY OF SIDEWALK AS STIPULATED', 'OCCUPANCY OF ROADWAY AS STIPULATED', 'CROSSING SIDEWALK',
               'REPAIR SIDEWALK', 'TEMPORARY PEDESTRIAN WALK', 'REPLACE SIDEWALK', 'SIDEWALK RECONSTRUCTION CONTRACTS',
               'SIDEWALK RECONSTRUCTION CONTRACTS-PROT', 'INSTALL TRAFFIC SIGNALS',
               'CONSTRUCT NEW SIDEWALK BLG. PAVEMENT',
               'REPAIR TRAFFIC STREET LIGHT', 'REPLACE/INSTALL STREET LIGHTS-PROTECTED',
               'OPEN SIDEWALK TO INSTALL FOUNDATION',
               'TRANSFORMER VAULT - IN SIDEWALK AREA', 'INSTALL STREET FURNITURE']
for i in permit_type:
    city_df.loc[city_df['PermitType'] == i, ['Corr']] = \
        numpy.corrcoef(city_df[city_df['PermitType'] == i]['PermitTypeDesc'], \
                       city_df[city_df['PermitType'] == i]['Accidents'])[0][1]

city_df.drop(['PermitTypeDesc', 'Accidents', 'BoroughName'], axis=1, inplace=True)
city_df = city_df.groupby(['PermitType', 'Borough']).mean().reset_index()

final_df = pd.concat([city_df, sample_df])
final_df.to_csv('Reconstruct_by borough.csv', index=False)