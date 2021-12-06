import pandas as pd
import pyam
from message_ix import make_df

def format_hist_cap(DATA):
    """This function creates a provincial historical capacity dataframe
    for MESSAGE
    DATA: Path to MESSAGEix data folder
    """
    
    # Read in gathered data
    exports = pd.read_excel(DATA / 'historical_capacity_WIP.xlsx',
                            sheet_name = 'electricity_exports',
                            usecols = ['region', 'technology',
                                       'year', 'value', 'unit'])

    powerplants = pd.read_excel(DATA / 'historical_capacity_WIP.xlsx',
                                sheet_name = 'powerplants',
                                usecols = ['region', 'technology',
                                           'year', 'value', 'unit'])

    # Concatenate dfs
    found = pd.concat([exports, powerplants])
    
    # Downscale other variables
    missing = ['biomass_t_d', 'coal_t_d', 'elec_t_d', 'foil_t_d',
               'gas_exp', 'gas_t_d', 'heat_t_d', 'loil_t_d']
    
    # Read in Canadian data as source for downscaling
    data = pd.read_excel('MESSAGE_CA__test__v4.xlsx',
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
    
    # Append downscaling proxies
    downscale = totals.append(powerplants)

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
                       unit = 'Gwa')

    return hist_cap