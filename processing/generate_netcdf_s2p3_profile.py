#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 18:48:25 2020

@author: polaris
"""

import glob
import pandas as pd
import sys
sys.path.append('../model/main/')
from run_map_parallel import output_type,include_depth_output,include_temp_surface_output,\
               include_temp_bottom_output,include_chlorophyll_surface_output,\
               include_phyto_biomass_surface_output,include_phyto_biomass_bottom_output,include_PAR_surface_output,\
               include_PAR_bottom_output,include_u_mean_surface_output,include_u_mean_bottom_output,include_grow1_mean_surface_output,\
               include_grow1_mean_bottom_output,include_uptake1_mean_surface_output,include_uptake1_mean_bottom_output,\
               include_windspeed_output,include_stressx_output,include_stressy_output,include_Etide_output,include_Ewind_output,\
               include_tpn1_output,include_tpg1_output,include_speed3_output,include_simpson_hunter_output,\
               include_temp_output,include_chlorophyll_output,include_phyto_biomass_output,\
               include_PAR_output,include_u_mean_output,include_grow1_mean_output,include_uptake1_mean_output,include_din_output
               
create_bathymetry = True

input_path = '../sims/test_profile/output/'
output_path = '../sims/test_profile/output/netcdf/'
file_id = 'test_profile'


if output_type == 1:
### Left here to later complete the 2D case
    columns = [include_depth_output,include_temp_surface_output,include_temp_bottom_output,include_chlorophyll_surface_output,\
               include_phyto_biomass_surface_output,include_phyto_biomass_bottom_output,include_PAR_surface_output,\
               include_PAR_bottom_output,include_u_mean_surface_output,include_u_mean_bottom_output,include_grow1_mean_surface_output,\
               include_grow1_mean_bottom_output,include_uptake1_mean_surface_output,include_uptake1_mean_bottom_output,\
               include_windspeed_output,include_stressx_output,include_stressy_output,include_Etide_output,include_Ewind_output,\
               include_tpn1_output,include_tpg1_output,include_speed3_output,include_simpson_hunter_output]
        
    column_names_all = ['depth','surface temperature','bottom temperature','surface chlorophyll',\
                        'surface phyto biomass','bottom phyto biomass','surface PAR','bottom PAR',\
                        'u_mean_surface','u_mean_bottom','grow1_mean_surface','grow1_mean_bottom',\
                        'uptake1_mean_surface','uptake1_mean_bottom','windspeed','stress_x','stress_y',\
                        'Etide','Ewind','tpn1','tpg1','speed3','simpson_hunter']
    #column_names = ['day','longitude','latitude']+list(compress(column_names_all, map(bool,columns)))



### Considering only the 3D case

if output_type == 2:

    variables_all = {'bathymetry': {'include':include_depth_output,'units':'m'},
                     'temperature': {'include':include_temp_output,'units':'°C'},
                     'chlorophyll': {'include':include_chlorophyll_output,'units':''},
                     'phyto biomass': {'include':include_phyto_biomass_output,'units':''},
                     'PAR': {'include':include_PAR_output,'units':''},
                     'u_mean': {'include':include_u_mean_output,'units':''},
                     'grow1_mean': {'include':include_grow1_mean_output,'units':''},
                     'uptake1_mean': {'include':include_uptake1_mean_output,'units':''},
                     'DIN': {'include':include_din_output,'units':'mmol m-3'},
                     'windspeed': {'include':include_windspeed_output,'units':''},
                     'stress_x': {'include':include_stressx_output,'units':''},
                     'stress_y': {'include':include_stressy_output,'units':''},
                     'Etide': {'include':include_Etide_output,'units':''},
                     'Ewind': {'include':include_Ewind_output,'units':''},
                     'tpn1': {'include':include_tpn1_output,'units':''},
                     'tpg1': {'include':include_tpg1_output,'units':''},
                     'speed3': {'include':include_speed3_output,'units':''},
                     'simpson_hunter': {'include':include_simpson_hunter_output,'units':''}}
    
    variables_sel = [k for k,par in variables_all.items() if par['include']==1] ## Selected variables
    bathymetry_var = 'bathymetry'
    
   
    
    dimensions = ['day','longitude','latitude','depth']    
    cols = dimensions + variables_sel    
    
    lon_to_180 = lambda x: x if x<=180 else x-360
    
    
    files = sorted(glob.glob(input_path + file_id+'_19[8-9]?'))
    
    for i,file in enumerate(files):    
        year = int(file.split('_')[-1])
        initialDate = pd.to_datetime(str(year-1)+'1231', format='%Y%m%d')
        df = pd.read_csv(file,header=None,names=cols, delim_whitespace=True)
        df['time'] = pd.to_timedelta(df['day'],unit='days') + initialDate    
        df['longitude'] = df.longitude.apply(lon_to_180)    
        df.drop(['day'], 1, inplace=True)
        ### Creates the bathymetry file and saves it as netcdf
        if i == 0 and bathymetry_var in cols and create_bathymetry:
            bath = df[(df.time==df.time.min())&(df.depth==df.depth.min())][['longitude','latitude',bathymetry_var]].reset_index(drop=True)
            bath.set_index(['latitude','longitude'], inplace=True)
            xr_bath = bath.to_xarray()
            xr_bath.bathymetry.attrs['units'] = 'm'        
            xr_bath.to_netcdf(f'{output_path}{bathymetry_var}_{file_id}.nc')
        
        df.set_index(['time','latitude','longitude','depth'], inplace=True)
        ## Creates the netcdf file for each variable
        for var in variables_sel:
            units = variables_all[var]['units']
            print(year,var)
            xr_temp = df[[var]].to_xarray()
            xr_temp[var].attrs['units'] = units
            xr_temp.depth.attrs['units'] = 'm'
            xr_temp.to_netcdf(f'{output_path}{var}_{file_id}_{year}.nc')
            del xr_temp
        del df