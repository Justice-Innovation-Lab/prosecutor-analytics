import pandas as pd
import numpy as np

# Define the number of samples
num_samples = 1000

# Define the percentages for each choice
guilty_percentage = 0.75
not_guilty_percentage = 0.25

# Define the possible values for each variable
races = ['White', 'Black', 'Hispanic', 'Asian', 'Other']
genders = ['Male', 'Female']
crime_types = ['Theft', 'Assault', 'Drug Possession', 'Fraud', 'Vandalism']
decisions = ['Guilty', 'Not Guilty']

# Generate synthetic data
data = {
    'race': np.random.choice(races, num_samples),
    'gender': np.random.choice(genders, num_samples),
    'crime_type': np.random.choice(crime_types, num_samples),
    'decision': np.random.choice(decisions, num_samples, p=[guilty_percentage, not_guilty_percentage])
}

# Create a DataFrame
df = pd.DataFrame(data)

# Display the first few rows of the DataFrame
print(df.head())
