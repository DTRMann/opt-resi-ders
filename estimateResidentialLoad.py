# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 11:21:31 2025

@author: DTRManning
"""

### TODO - remove row-wise multiplication and do multiplication as dataframe columns

import numpy as np
import pandas as pd

class ModelFactors:
    """Scaling factors and fundamental parameters"""
    # House size scaling
    size_reference: float = 2000  # Reference house size in sq ft
    size_exponent: float = 0.7    # Non-linear scaling exponent
    
    # Behavioral harmonics (24-hour pattern)
    base_load: float = 1  # Base load in kW
    # First harmonic (24-hour cycle)
    harmonic1_amplitude: float = 0.4
    harmonic1_phase: float = -1.8  # Shifts peak towards evening
    # Second harmonic (12-hour cycle)
    harmonic2_amplitude: float = 0.25
    harmonic2_phase: float = -0.8  # Adjusts morning/evening balance
    
    # Time-based multipliers
    weekend_holiday_multiplier: float = 1.2
    seasonal_amplitude: float = 0.2

class WeatherCoefficients:
    """Weather sensitivity coefficients that can be tuned per house type"""
    temp_base: float = 0.05  # kW per degree F (linear term)
    temp_heat: float = 0.2  # kW per degree F below 60F
    temp_cool: float = 0.1  # kW per degree F above 60F
    irradiance: float = -0.001  # kW per W/m2

class HybridLoadModel:
    def __init__(
        self,
        factors: ModelFactors = None,
        weather_coeffs: WeatherCoefficients = None,
        reference_temp: float = 60.0
    ):
        self.factors = ModelFactors()
        self.weather_coeffs = WeatherCoefficients()
        self.reference_temp = reference_temp
        
    def _calculate_size_factor(self, house_size: float) -> float:
        """Calculate house size scaling factor"""
        return (house_size / self.factors.size_reference) ** self.factors.size_exponent
    
    
    def _calculate_hourly_pattern(self, hour: float) -> float:
        """
        Calculate behavioral load pattern using harmonic terms
        hour: float from 0 to 23
        """
        # Convert hour to radians (2π represents 24 hours)
        t = 2 * np.pi * hour / 24
        
        # Combine multiple harmonics for complex pattern
        pattern = (
            self.factors.base_load + 
            self.factors.harmonic1_amplitude * np.sin(t + self.factors.harmonic1_phase) +
            self.factors.harmonic2_amplitude * np.sin(2*t + self.factors.harmonic2_phase) 
        )
        
        # Ensure non-negative load
        return max(pattern, 0.1 * self.factors.base_load)
    
    def _calculate_weather_load(
        self,
        temperature: float,
        irradiance: float
    ) -> float:
        """Calculate weather-dependent load component"""
        # Base temperature response
        load = self.weather_coeffs.temp_base * temperature
        
        # Heating load (when below reference temp)
        if temperature < self.reference_temp:
            load += self.weather_coeffs.temp_heat * (self.reference_temp - temperature)
            
        # Cooling load (when above reference temp)
        if temperature > self.reference_temp:
            load += self.weather_coeffs.temp_cool * (temperature - self.reference_temp)
            
        # Solar impact
        load += self.weather_coeffs.irradiance * irradiance
        
        return load
    
    def _calculate_behavioral_load(
        self,
        hour: int,
        is_weekend: bool,
        is_holiday: bool,
        day_of_year: int
    ) -> float:
        """Calculate behavioral load component"""
        # Get base hourly pattern
        load = self._calculate_hourly_pattern(hour)
        
        # Weekend/holiday adjustment
        if is_holiday or is_weekend:
            load *= self.factors.weekend_holiday_multiplier
            
        # Seasonal variation using sinusoidal pattern
        seasonal_factor = 1.0 + self.factors.seasonal_amplitude * \
            np.sin(2 * np.pi * (day_of_year - 45) / 365)  # Phase shift for winter peak
        load *= seasonal_factor
        
        return load
    
    def predict_load(
        self,
        df: pd.DataFrame,
        house_size: float
    ) -> pd.Series:
        """
        Predict hourly load based on weather and time features
        
        Parameters:
        -----------
        df : pd.DataFrame
            Must contain columns: temperature, irradiance, is_weekend, is_holiday
            Must have datetime index
        house_size : float
            House size in square feet
            
        Returns:
        --------
        pd.Series
            Predicted hourly load in kW
        """
        # Calculate scaling factors
        size_factor = self._calculate_size_factor(house_size)
        
        loads = []
        for idx, row in df.iterrows():
            # Weather component
            weather_load = self._calculate_weather_load(
                row['temperature'],
                row['irradiance']
            )
            
            # Behavioral component
            behavioral_load = self._calculate_behavioral_load(
                idx.hour,
                row['is_weekend'],
                row['is_holiday'],
                idx.dayofyear
            )
            
            # Combine components with scaling factors
            total_load = max((weather_load + behavioral_load), 0.25 * self.factors.base_load) # Ensure load are positive
            loads.append(total_load)
         
        loads = pd.DataFrame({'Load': loads}, index=df.index)            
        loads['Load'] = loads['Load'] * size_factor
            
        return loads

# Example usage and visualization
if __name__ == "__main__":
    import matplotlib.pyplot as plt
    
    # Create sample data for one day
    dates = pd.date_range('2024-01-01', '2024-01-02', freq='H')[:-1]  # 24 hours
    df = pd.DataFrame(index=dates)
    
    # Sample weather data
    df['temperature'] = 65 + 15 * np.sin(2 * np.pi * df.index.hour / 24)
    df['irradiance'] = np.maximum(0, 1000 * np.sin(2 * np.pi * (df.index.hour - 6) / 24))
    df['is_weekend'] = df.index.weekday >= 5
    df['is_holiday'] = False
    
    # Create model and predict
    model = HybridLoadModel()
    predictions = model.predict_load(
        df,
        house_size=2000
    )
    
    # Plot daily pattern
    plt.figure(figsize=(12, 6))
    plt.plot(range(24), predictions.values)
    plt.title('24-Hour Load Profile')
    plt.xlabel('Hour of Day')
    plt.ylabel('Load (kW)')
    plt.grid(True)
    plt.show()