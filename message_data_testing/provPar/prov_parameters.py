import pandas as pd

def generator(msg):
    """This function creates a dictionary of provincial parameter dataframes by copying national ones
    
       msg: message_ix.Scenario
    """
    # Province names for creating dataframes
    provinces = ['Alberta',
             'BritishColumbia',
             'Manitoba',
             'NewBrunswick',
             'NewfoundlandandLabrador',
             'NorthwestTerritories',
             'NovaScotia',
             'Nunavut',
             'Ontario',
             'PrinceEdwardIsland',
             'Quebec',
             'Saskatchewan',
             'Yukon']
    
    # All (non-empty) parameters of the Canadian model
    total_pars = [par for par in msg.par_list() if not msg.par(par).empty]

    # Parameters already found with data
    found_pars = ['demand', 'historical_activity', 'historical_new_capacity']

    # Economic parameters to skip for now
    econ_pars = ['MERtoPPP', 
             'abs_cost_activity_soft_lo',
             'abs_cost_activity_soft_up',
             'cost_MESSAGE',
             'depr',
             'drate',
             'esub',
             'fix_cost',
             'gdp_calibrate',
             'grow',
             'interestrate',
             'inv_cost',
             'kgdp',
             'kpvs',
             'level_cost_activity_soft_lo',
             'level_cost_activity_soft_up',
             'level_cost_new_capacity_soft_up',
             'price_MESSAGE',
             'relation_cost',
             'resource_cost',
             'var_cost']

    # Filter out un-needed parameters
    filtered = [x for x in total_pars if x not in found_pars if x not in econ_pars]
    filtered.sort()
    
    # Create dictionary to store parameter : <columns containing 'node'> pairs
    pars_to_change = {}

    # Iterate through parameters
    for par in filtered:
        
        # Create lists for appending 'node' and 'year' containing columns
        node_stuff = []
        year_stuff = []

            # Iterate through columns of parameter dataframe
            for col in msg.par(par).columns:

                # Check for node column (or some variation including string 'node')
                if 'node' in col:
                    node_stuff.append(col)
                    
                # Check for year column, make sure it contains integers
                if 'year' in col and msg.par(par).dtypes[col] == 'int64':
                    year_stuff.append(col)

            if len(node_stuff) != 0:
                # Add parameter and column values to dictionary
                pars_to_change[par] = [node_stuff, year_stuff]
    
    # Create dictionary with key = <parameter name> and value = provincial dataframe
    dfs = dict.fromkeys(pars_to_change.keys())

    for par in dfs:    
        # Copy canadian dataframe
        can_df = msg.par(par).copy()
        
        # Only need data for years after (and including) 2010
        for year_data in pars_to_change[par][1]:
            can_df = can_df[can_df[year_data] >= 2010]

        for prov in provinces:
            
            # Create copy of Canadian dataframe
            df = msg.par(par).copy()

            # Only need data for years after (and including) 2010
            for year_data in pars_to_change[par][1]:

                df = df[df[year_data] >= 2010]

            # Change node values to province
            for node_data in pars_to_change[par][0]:
                df[node_data] = region

            # Append provincial data to parameter dataframe
            dfs[par] = dfs[par].append(df)
    
    return dfs