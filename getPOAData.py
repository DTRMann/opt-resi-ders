# -*- coding: utf-8 -*-
"""
@author: DTRM
"""

import pandas as pd
import os

def calculate_poa(df: pd.DataFrame,
                 soiling_factor: float = 0.98,
                 iam_factor: float = 0.95,
                 spectral_factor: float = 0.97,
                 shading_factors: pd.Series = None,
                 gti_column: str = 'gti') -> pd.DataFrame:
    """
    Calculate Plane of Array (POA) irradiance from GTI data considering optical losses and hourly shading
    
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
    shading_factors : pd.Series
        Series of shading factors by hour (default None = no shading)
    gti_column : str
        Name of the column containing GTI data (default 'gti')
        
    Returns:
    --------
    pandas.DataFrame
        Original DataFrame with added POA calculations
    """
    # Make a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # Apply default shading factor if not provided
    if shading_factors is None:
        shading_factors = pd.Series(1.0, index=result_df.index)
    
    # Calculate POA with losses including hourly shading
    result_df['poa_irradiance'] = (
        result_df[gti_column] * 
        soiling_factor * 
        iam_factor * 
        spectral_factor * 
        shading_factors  # Hourly shading factors
    )
    
    # Calculate individual loss components for analysis
    result_df['soiling_loss'] = result_df[gti_column] * (1 - soiling_factor)
    result_df['iam_loss'] = result_df[gti_column] * (1 - iam_factor)
    result_df['spectral_loss'] = result_df[gti_column] * (1 - spectral_factor)
    result_df['shading_loss'] = result_df[gti_column] * (1 - shading_factors)
    
    # Calculate total losses
    result_df['total_losses'] = (
        result_df['soiling_loss'] + 
        result_df['iam_loss'] + 
        result_df['spectral_loss'] + 
        result_df['shading_loss']
    )
    
    
    return result_df

# Example usage
if __name__ == "__main__":
    # Read test GTI data
    wd = 'C:\\Users\\DTRManning\\Desktop\\OptimizeResiGenSizing'
    filepath = os.path.join(wd, 'testGTIData.csv')
    sample_data = pd.read_csv( filepath )
    
    # Calculate POA with default loss factors
    result_df = calculate_poa(sample_data)
        
result_df.to_csv( 'C:\\Users\\DTRManning\\Desktop\\OptimizeResiGenSizing\\testPOAData.csv', index=True )
        