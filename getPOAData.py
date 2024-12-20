# -*- coding: utf-8 -*-
"""
@author: DTRM
"""

import pandas as pd
import numpy as np
import os

def calculate_poa(df: pd.DataFrame,
                 soiling_factor: float = 0.98,
                 iam_factor: float = 0.95,
                 spectral_factor: float = 0.97,
                 gti_column: str = 'gti') -> pd.DataFrame:
    """
    Calculate Plane of Array (POA) irradiance from GTI data considering optical losses
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing hourly GTI data
    soiling_factor : float
        Reduction due to soiling/dust (default 0.98 = 2% loss)
    iam_factor : float
        Incidence Angle Modifier factor (default 0.95 = 5% loss)
    spectral_factor : float
        Spectral loss factor (default 0.97 = 3% loss)
    gti_column : str
        Name of the column containing GTI data (default 'gti')
        
    Returns:
    --------
    pandas.DataFrame
        Original DataFrame with added POA calculations
    """
    # Make a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # Calculate POA with losses
    result_df['poa_irradiance'] = (
        result_df[gti_column] * 
        soiling_factor * 
        iam_factor * 
        spectral_factor
    )
    
    # Calculate individual loss components for analysis
    result_df['soiling_loss'] = result_df[gti_column] * (1 - soiling_factor)
    result_df['iam_loss'] = result_df[gti_column] * (1 - iam_factor)
    result_df['spectral_loss'] = result_df[gti_column] * (1 - spectral_factor)
    
    # Calculate total losses
    result_df['total_losses'] = (
        result_df['soiling_loss'] + 
        result_df['iam_loss'] + 
        result_df['spectral_loss']
    )
    
    # Calculate loss percentages
    total_gti = result_df[gti_column].sum()
    loss_summary = {
        'Total GTI (Wh/m²)': total_gti,
        'Total POA (Wh/m²)': result_df['poa_irradiance'].sum(),
        'Soiling Losses (%)': (result_df['soiling_loss'].sum() / total_gti * 100),
        'IAM Losses (%)': (result_df['iam_loss'].sum() / total_gti * 100),
        'Spectral Losses (%)': (result_df['spectral_loss'].sum() / total_gti * 100),
        'Total Losses (%)': (result_df['total_losses'].sum() / total_gti * 100)
    }
    
    return result_df, loss_summary

# Example usage
if __name__ == "__main__":
    # Read test GTI data
    wd = ''
    filepath = os.path.join(wd, 'testGTIData.csv')
    sample_data = pd.read_csv( filepath )
    
    dates = pd.date_range(start='2024-01-01', end='2024-01-02', freq='H')
    sample_data = pd.DataFrame({
        'timestamp': dates,
        'gti': np.random.uniform(0, 1000, len(dates))  # Sample GTI values
    }).set_index('timestamp')
    
    # Calculate POA with default loss factors
    result_df, losses = calculate_poa(sample_data)
    
    print("\nSample of hourly data:")
    print(result_df[['gti', 'poa_irradiance', 'total_losses']].head())
    
    print("\nLoss Summary:")
    for key, value in losses.items():
        print(f"{key}: {value:.2f}")
        

        