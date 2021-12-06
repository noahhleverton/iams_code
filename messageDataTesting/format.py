import pandas as pd
import pyam
from message_ix import make_df
from pathlib import Path

def format_demand(DATA):
    """This function gathers demand data for MESSAGE
    
    DATA : Path to MESSAGEix data folder
    
    returns .csv in MESSAGE format
    """
    
    # Basic model data
    MODEL = 'MESSAGEix'
    SCEN = 'None'
    MSG_YEARS = list(range(2020, 2061, 5))
    
    # Read in data for specific and transport demand
    spec = pyam.IamDataFrame(DATA / 'provincial_sources' / 'can_energy_future.xlsx', 
                             model = MODEL, 
                             scenario = SCEN, 
                             region = 'node')
    
    # List of variable name changes
    spec.rename(variable = {'Transportation|Total End-Use': 'transport',
                            'Residential|Electricity': 'sp_el_r',
                            'Commercial|Electricity': 'sp_el_c',
                            'Residential|Biofuels & Emerging Energy': 'solar_pv_r',
                            'Commercial|Biofuels & Emerging Energy': 'solar_pv_c',
                            'Industrial|Electricity': 'sp_el_I',
                            'Industrial|Biofuels & Emerging Energy': 'solar_pv_I'},
                            inplace = True)
            
    # Aggregate variables to get specific res/comm and tranport demands
    spec.add('solar_pv_r', 'solar_pv_c', 'solar_pv_rc', ignore_units = 'PJ', append = True)
    spec.add('sp_el_r', 'sp_el_c', 'sp_el_rc', ignore_units = 'PJ', append = True)
    spec.add('sp_el_rc', 'solar_pv_rc', 'rc_spec', ignore_units = 'PJ', append = True)
    spec.add('sp_el_I', 'solar_pv_I', 'i_spec', ignore_units = 'PJ', append = True)
    
    # Filter to only aggregated variables
    spec = spec.filter(variable = ['rc_spec', 'i_spec', 'transport'])
    
    # Convert units
    spec = spec.convert_unit('PJ', to = 'GWh').convert_unit('GWh', to = 'GWa', factor = 1/8760)
    
    # Read in Canadian demand data from previous model run (for downscaling)
    can_data = pd.read_excel(DATA / 'MESSAGE_CA__test__v4.xlsx',
                             sheet_name = 'demand',
                             usecols=['node', 'commodity', 'year', 'unit', 'value'])
    
    # Convert to IAM format
    can_data = pyam.IamDataFrame(can_data,
                                 model = MODEL,
                                 scenario = SCEN,
                                 region = 'node',
                                 variable = 'commodity'
                                 ).filter(variable = ['i_feed', 'rc_therm', 'i_therm'], year = MSG_YEARS)
    
    # Thermal downscaling
    therm = can_data.append(spec)
    i_therm = therm.downscale_region('i_therm', region = 'Canada', proxy = 'i_spec')
    rc_therm = therm.downscale_region('rc_therm', region = 'Canada', proxy = 'rc_spec')
    
    # Feedstock downscaling
    feed = can_data.append(spec)
    i_feed = feed.downscale_region('i_feed', region = 'Canada', proxy = 'i_spec')
    
    # Gather all data together
    total = spec
    total.append(can_data, inplace = True)
    total.append(i_therm, inplace = True)
    total.append(rc_therm, inplace = True)
    total.append(i_feed, inplace = True)
    
    # Change provinces names 
    total.rename(region = {'British Columbia': 'BritishColumbia',
                           'New Brunswick': 'NewBrunswick',
                           'Newfoundland and Labrador': 'NewfoundlandandLabrador',
                           'Northwest Territories': 'NorthwestTerritories',
                           'Nova Scotia': 'NovaScotia',
                           'Prince Edward Island': 'PrinceEdwardIsland'}, inplace = True)
    
    # Filter to required year range
    total = total.filter(year = MSG_YEARS)
    
    
    demand = total.timeseries()
    
    # Fill in 2055/2060 with growth rate from previous years
    demand[2055] = demand[2050]*demand[2050]/demand[2045]
    demand[2060] = demand[2055]*demand[2055]/demand[2050]
    
    # Sort dataframe
    demand = demand.reset_index()
    demand.drop(columns = ['scenario', 'model'], inplace = True)
    demand = demand.melt(id_vars = ['region', 'variable', 'unit'], var_name = 'year', value_name = 'value')
    demand = demand.set_index(['region', 'variable', 'year', 'unit']).sort_index().reset_index()

    # Create MESSAGE compatible dataframe
    dmd = make_df('demand',
                  node = demand['region'],
                  commodity = demand['variable'],
                  year = demand['year'],
                  time = 'year',
                  level = 'useful',
                  value = demand['value'],
                  unit = 'Gwa')
    
    dmd.to_csv(DATA / 'demand.csv')

def format_hist_act(DATA):
    """ This function gathers historical activity data for MESSAGE
    
        DATA: path to MESSAGEix Canada data folder
        
        returns .csv in MESSAGE format
    """
    
    MODEL = 'MESSAGEix'
    SCEN = 'none'
    
    cols = ['region', 'technology', 'year', 'value', 'unit']
    
    ppl = pd.read_excel(DATA / 'provincial_sources' / 'historical_activity.xlsx',
                        sheet_name = 'powerplants',
                        usecols = cols,
                       ).dropna()
    
    therm = pd.read_excel(DATA / 'provincial_sources' / 'historical_activity.xlsx',
                          sheet_name = 'thermal',
                          usecols = cols,
                         ).dropna()
    
    other_elec = pd.read_excel(DATA / 'provincial_sources' / 'historical_activity.xlsx',
                               sheet_name = 'other_electricity',
                               usecols = cols,
                              ).dropna()
    
    prod = pd.read_excel(DATA / 'provincial_sources' / 'historical_activity.xlsx',
                         sheet_name = 'production',
                         usecols = cols,
                        ).dropna()
    
    # Concatenate data
    total = pd.concat([ppl, therm, other_elec, prod],
                      axis = 'rows').set_index('region').sort_index()
    
    # Create IAM df
    pyam_total = pyam.IamDataFrame(total,
                                   model = MODEL,
                                   scenario = SCEN,
                                   variable = 'technology')
    
    # Unit conversion
    pyam_total.convert_unit('PJ', to = 'GWa', inplace = True)
    pyam_total.rename(unit = {'GWa': 'Gwa'}, inplace = True)
    
    # Downscale data with only Canadian values
    can_vars = [x for x in pyam_total.filter(region = 'Canada').variable if x not in pyam_total.filter(region = 'Ontario').variable]
    can_vars.sort()
    
    # Create dictionary to categorize variables
    vars_dict = {'coal': [x for x in can_vars if x.split('_')[0] == 'coal'],
                 'gas': [x for x in can_vars if x.split('_')[0] == 'gas'],
                 'oil': [x for x in can_vars if x.split('_')[0] == 'oil']}
    
    for value in vars_dict['coal']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'coal_ppl', append = True)
            
    for value in vars_dict['gas']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'gas_ppl', append = True)
        
    for value in vars_dict['oil']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'foil_ppl', append = True)
    
    # Read in Canadian data
    data = pd.read_excel(DATA / 'MESSAGE_CA__test__v4.xlsx',
                             sheet_name = 'historical_activity',
                             usecols = ['node_loc', 'technology', 'year_act',
                                        'value', 'unit'])
    
    # Sort to only include year = 2015, non-zero values
    msg_can = data[(data['year_act']  == 2015) &
                        (data['value'] != 0)].reset_index(drop = True)
    
    total_vars = msg_can['technology'].values
    
    # Filter to only include missing variables
    downscale_vars = [x for x in msg_can['technology'].unique() if x not in pyam_total.variable]
    msg_can = msg_can[msg_can['technology'].isin(downscale_vars)]
    
    # Create IAM df
    pyam_msg_can = pyam.IamDataFrame(msg_can,
                                     model = MODEL,
                                     scenario = SCEN,
                                     region = 'node_loc',
                                     variable = 'technology',
                                     year = 'year_act')
    
    pyam_msg_can.rename(unit = {'GWa': 'Gwa'}, inplace = True)
    
    # Read in gdp data for downscaling
    gdp = pd.read_excel(DATA / 'provincial_sources' / 'pop_gdp_proj.xlsx',
                        sheet_name = 'gdp_pop_proj')
    
    gdp = gdp[(gdp['variable'] == 'GDP (PPP)') & 
              (gdp['year'] == 2015)].reset_index(drop = True)
    
    pyam_gdp = pyam.IamDataFrame(gdp,
                                 model = MODEL,
                                 scenario = SCEN)
    
    pyam_total.append(pyam_msg_can, inplace = True)
    pyam_total.append(pyam_gdp, inplace = True)
    
    # Create dictionary to categorize variables
    biomass = [x for x in downscale_vars if x.split('_')[0] == 'biomass']
    coal = [x for x in downscale_vars if x.split('_')[0] == 'coal']
    elec = [x for x in downscale_vars if x.split('_')[0] == 'elec']
    foil = [x for x in downscale_vars if x.split('_')[0] == 'foil']
    gas = [x for x in downscale_vars if x.split('_')[0] == 'gas']
    loil = [x for x in downscale_vars if x.split('_')[0] == 'loil']
    other = [x for x in downscale_vars if x not in set(biomass + coal + elec + foil + gas + loil)]
    
    
    downscale_dict = {'biomass': biomass,
                      'coal': coal,
                      'elec': elec,
                      'foil': foil,
                      'gas': gas,
                      'loil': loil,
                      'other': other}
    
    for value in downscale_dict['biomass']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'bio_ppl', append = True)
        
    for value in downscale_dict['coal']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'coal_ppl', append = True)
        
    for value in downscale_dict['elec']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'elec_exp', append = True)
    
    for value in downscale_dict['foil']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'foil_ppl', append = True)
    
    for value in downscale_dict['gas']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'gas_ppl', append = True)
    
    for value in downscale_dict['loil']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'loil_ppl', append = True)
    
    for value in downscale_dict['other']:
        pyam_total.downscale_region(value, region = 'Canada', proxy = 'GDP (PPP)', append = True)
    
    final = pyam_total.filter(variable = total_vars)
    
    final.to_csv(DATA / 'historical_activity.csv')

def format_hist_cap(DATA):
    """ This function gathers historical capacity data for MESSAGE
    
        DATA: path to MESSAGEix Canada data folder
        
        returns .csv in MESSAGE format
    """
    
    # Read in gathered data
    exports = pd.read_excel(DATA / 'provincial_sources' / 'historical_capacity.xlsx',
                            sheet_name = 'electricity_exports',
                            usecols = ['region', 'technology',
                                       'year', 'value', 'unit'])

    powerplants = pd.read_excel(DATA / 'provincial_sources' / 'historical_capacity.xlsx',
                                sheet_name = 'powerplants',
                                usecols = ['region', 'technology',
                                           'year', 'value', 'unit'])

    # Concatenate dfs
    found = pd.concat([exports, powerplants])
    
    # Downscale other variables
    missing = ['biomass_t_d', 'coal_t_d', 'elec_t_d', 'foil_t_d',
               'gas_exp', 'gas_t_d', 'heat_t_d', 'loil_t_d']
    
    # Read in Canadian data as source for downscaling
    data = pd.read_excel(DATA / 'MESSAGE_CA__test__v4.xlsx',
                             sheet_name = 'historical_new_capacity',
                             usecols = ['node_loc', 'technology',
                                        'year_vtg', 'value', 'unit'])

    # Sort to only include missing variables
    can_data = data[(data['technology'].isin(missing))].copy()

    # Match naming
    can_data.rename(columns = {'node_loc': 'region',
                               'year_vtg': 'year'}, inplace = True)
    can_data['unit'] = 'Gwa'

    can_data.set_index('technology', inplace = True)
    can_data.sort_index()

    # Create totals in 2015, as the gathered data is TOTAL installed
    # capacity, and can_data is NEW capacity in a given year
    totals = can_data.groupby('technology').sum()
    totals['region'] = 'Canada'
    totals['year'] = 2015
    totals['unit'] = 'Gwa'
    totals.reset_index(inplace = True)
    totals = totals[['region', 'technology', 'year', 'value', 'unit']]
    
    # Append found data
    downscale = totals.append(found)

    # Change to pyam df to use downscaling method
    down_iam = pyam.IamDataFrame(downscale, model = 'MESSAGEix',
                                 scenario = 'none',
                                 variable = 'technology')
    
    # Downscaling
    down_iam.downscale_region('biomass_t_d', region = 'Canada', proxy = 'bio_ppl', append = True)
    down_iam.downscale_region('coal_t_d', region = 'Canada', proxy = 'coal_ppl', append = True)
    down_iam.downscale_region('elec_t_d', region = 'Canada', proxy = 'hydro_lc', append = True)
    down_iam.downscale_region('foil_t_d', region = 'Canada', proxy = 'foil_ppl', append = True)
    down_iam.downscale_region('gas_exp', region = 'Canada', proxy = 'gas_ppl', append = True)
    down_iam.downscale_region('gas_t_d', region = 'Canada', proxy = 'gas_ppl', append = True)
    down_iam.downscale_region('heat_t_d', region = 'Canada', proxy = 'hydro_lc', append = True)
    down_iam.downscale_region('loil_t_d', region = 'Canada', proxy = 'loil_ppl', append = True)
    
    down = down_iam.as_pandas()
    down = down[['region', 'variable', 'year', 'value', 'unit']]
    
    # Create MESSAGE compatible df
    hist_cap = make_df('historical_new_capacity',
                       node_loc = down['region'],
                       technology = down['variable'],
                       year_vtg = down['year'],
                       value = down['value'],
                       unit = 'GW')
    
    hist_cap.to_csv(DATA / 'historical_capacity.csv')

def main():
    
    DATA = Path('/home/noah/message-ix/data')
    format_demand(DATA)
    format_hist_act(DATA)
    format_hist_cap(DATA)

if __name__ == '__main__':
    main()
