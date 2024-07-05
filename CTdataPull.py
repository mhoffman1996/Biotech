import requests
import pandas as pd
import numpy as np
import openpyxl
import os 
from datetime import datetime
import xlsxwriter

# Read in list of assets of interest and convert to list
assetFrame = pd.read_excel('assetList3.xlsx')
assetList = assetFrame['Asset'].tolist()
print('Asset list successfully imported')

# Initialize an empty list to store the data
data_list = []

for asset in assetList:
    # Initial URL for the first API call
    base_url = "https://clinicaltrials.gov/api/v2/studies"
    params = {
        "query.term": asset,
        "pageSize": 100
    }
    # Assign asset name from parameter
    assetName = params['query.term']

    # Loop until there is no nextPageToken
    while True:
        # Print the current URL (for debugging purposes); Access by looping parameters
        print("Fetching data from:", base_url + '?' + '&'.join([f"{k}={v}" for k, v in params.items()]))
        
        # Send a GET request to the API
        response = requests.get(base_url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()  # Parse JSON response
            studies = data.get('studies', [])  # Extract the list of studies, pass empty parameters

            # Loop through each study and extract specific information
            for study in studies:
                # Safely access nested keys, Indicates Unknown if unavailable 
                nctId = study['protocolSection']['identificationModule'].get('nctId', 'Unknown')
                studyTitle = study['protocolSection']['identificationModule'].get('officialTitle', 'Unknown')
                sponsor = study['protocolSection']['sponsorCollaboratorsModule'].get('leadSponsor', {}).get('name', 'Unknonw Lead Sponsor')
                overallStatus = study['protocolSection']['statusModule'].get('overallStatus', 'Unknown')
                enrollment = study['protocolSection']['designModule'].get('enrollmentInfo',{}).get('count', 'NaN')
                startDate = study['protocolSection']['statusModule'].get('startDateStruct', {}).get('date', 'Unknown Date')

                # Some CTs have no active conditions module, probably a more elegant way to check for this, but including as solution for now
                if 'conditionsModule' in study['protocolSection']:
                    conditions = ', '.join(study['protocolSection']['conditionsModule'].get('conditions', ['No conditions listed']))
                else: conditions = 'No conditions identified'

                # Safely access and store acronym
                acronym = study['protocolSection']['identificationModule'].get('acronym', 'Unknown')

                # Extract interventions safely
                interventions_list = study['protocolSection'].get('armsInterventionsModule', {}).get('interventions', [])
                interventions = ', '.join([intervention.get('name', 'No intervention name listed') for intervention in interventions_list]) if interventions_list else "No interventions listed"

                # Extract outcomes safely
                primaryOutcomes_list = study['protocolSection'].get('outcomesModule', {}).get('primaryOutcomes', [])
                primaryOutcomes = ', '.join([measure.get('measure', 'No primary outcome indicated') for measure in primaryOutcomes_list]) if primaryOutcomes_list else "No primary outcomes listed"
                secondaryOutcomes_list = study['protocolSection'].get('outcomesModule', {}).get('secondaryOutcomes', [])
                secondaryOutcomes = ', '.join([measure.get('measure', 'No secomdary outcomes indicated') for measure in secondaryOutcomes_list]) if secondaryOutcomes_list else "No secondary outcomes listed"
                
                # Extract locations safely
                locations_list = study['protocolSection'].get('contactsLocationsModule', {}).get('locations', [])
                locations = ', '.join([f"{location.get('city', 'No City')} - {location.get('country', 'No Country')}" for location in locations_list]) if locations_list else "No locations listed"
                
                # Extract dates and phases
                primaryCompletionDate = study['protocolSection']['statusModule'].get('primaryCompletionDateStruct', {}).get('date', 'Unknown Date')
                studyFirstPostDate = study['protocolSection']['statusModule'].get('studyFirstPostDateStruct', {}).get('date', 'Unknown Date')
                lastUpdatePostDate = study['protocolSection']['statusModule'].get('lastUpdatePostDateStruct', {}).get('date', 'Unknown Date')
                studyType = study['protocolSection']['designModule'].get('studyType', 'Unknown')
                phases = ', '.join(study['protocolSection']['designModule'].get('phases', ['Not Available']))

                # Append the data to the list as a dictionary
                data_list.append({
                    "NCT ID": nctId,
                    "Asset": assetName,
                    "Title": studyTitle,
                    "Lead Sponsor": sponsor,
                    "Acronym": acronym,
                    "Overall Status": overallStatus,
                    "Enrollment": enrollment,
                    "Start Date": startDate,
                    "Conditions": conditions,
                    "Interventions": interventions,
                    "Primary Outcome(s)": primaryOutcomes,
                    "Secondary Outcome(s)": secondaryOutcomes,
                    "Locations": locations,
                    "Primary Completion Date": primaryCompletionDate,
                    "Study First Post Date": studyFirstPostDate,
                    "Last Update Post Date": lastUpdatePostDate,
                    "Study Type": studyType,
                    "Phases": phases
                })

            # Check for nextPageToken and update the params or break the loop
            nextPageToken = data.get('nextPageToken')
            if nextPageToken:
                params['pageToken'] = nextPageToken  # Set the pageToken for the next request
            else:
                break  # Exit the loop if no nextPageToken is present
        else:
            print("Failed to fetch data. Status code:", response.status_code)
            break

# Create a DataFrame from the list of dictionaries and make sure enrollment is a numeric value and cleaning phases variable to be Not available.
currentDataFrame = pd.DataFrame(data_list)
currentDataFrame['Enrollment'] = pd.to_numeric(currentDataFrame['Enrollment'], errors = 'coerce')
currentDataFrame['Phases'] = currentDataFrame['Phases'].replace({'NA': 'Not Available'})

# Index new data frame 
currentDataFrame.set_index('NCT ID', inplace = True)

# Function to concatenate Asset column for NCTs with multiple relevant assets
def resolveConflicts(group):
    concatenatedAsset = ', '.join(group['Asset'].astype(str))
    # Take the first row's data for other columns (you can modify this logic as needed)
    firstRow = group.iloc[0]
    firstRow['Asset'] = concatenatedAsset
    return firstRow

# Reset the index before grouping and set it back afterward
currentDataFrame = currentDataFrame.reset_index().groupby('NCT ID', group_keys=False).apply(resolveConflicts).reset_index(drop=True).set_index('NCT ID')

# Identify whether comparator trials excel exists
workingDirectory = os.getcwd()
isExisting = os.path.exists(workingDirectory+'/oldClinicalTrialsData.csv')

# If comparator trial exists already make comparator data frame, otherwise initialize excel
if isExisting == True:
    
    newClinTrialExists = os.path.exists(workingDirectory + '/newClinicalTrialsData.csv')
    if newClinTrialExists == True:

        # Remove previous old Clinical trials csv, rename the old 'new csv' as old to roll over for new week
        os.remove(workingDirectory +'/oldClinicalTrialsData.csv')
        os.rename(workingDirectory +'/newClinicalTrialsData.csv', workingDirectory + '/oldClinicalTrialsData.csv')
        print('\n\nPrevious clinical trial data identified. We have rolled this data over for a new weekly comparison of changes')

    else: 
        print('\n\nScanning file path to identify previous historic data.')
        print('\n\nOnly old historic data was identified. Generating new historic data .csv for comparison.')

    # Save new data frame as a csv
    currentDataFrame.to_csv("newClinicalTrialsData.csv")

    # Creating old data frame from existing excel
    oldDataDirectory = workingDirectory + '/oldClinicalTrialsData.csv'
    oldDataFrame = pd.read_csv(oldDataDirectory)

    # Make sure old data enrollment variable is a numerical and cleaning phase data to be Not Available
    oldDataFrame['Enrollment'] = pd.to_numeric(oldDataFrame['Enrollment'], errors = 'coerce')
    oldDataFrame['Phases'] = oldDataFrame['Phases'].replace({np.nan: 'Not Available'})

    # Index old data frame and drop duplicates
    oldDataFrame.set_index('NCT ID', inplace = True)
    print('Historic clinical trials data recognized and reconciling changes from historic file and new trial updates.')
    
    # Create dataframes for rows that were added or deleted 
    addedRows = currentDataFrame[~currentDataFrame.index.isin(oldDataFrame.index)]
    deletedRows = oldDataFrame[~oldDataFrame.index.isin(currentDataFrame.index)]
    
    # Find modified rows by finding intersection of old and new data frames and masking based on that intersection
    commonIndex = oldDataFrame.index.intersection(currentDataFrame.index)
    oldDataCommon = oldDataFrame.loc[commonIndex]
    newDataCommon = currentDataFrame.loc[commonIndex]
    modifiedMask = pd.Series(False, index = commonIndex)

    # Compare data frames to determine whether differences exist
    for col in oldDataFrame.columns:
        if np.issubdtype(oldDataFrame[col].dtype, np.number):
            # Compare numerical columns with tolerance and create mask 
            difference = np.abs(oldDataCommon[col] - newDataCommon[col])
            mask = difference > 1e-9
            modifiedMask = modifiedMask | mask

        else:
            # Compare non-numerical columns directly and create mask 
            difference = oldDataCommon[col] != newDataCommon[col]
            modifiedMask = modifiedMask | difference
    
    # Initialize data frames for old and new data frames selected for differences and comparing
    oldModifiedRows = oldDataCommon[modifiedMask]
    newModifiedRows = newDataCommon[modifiedMask]
    comparedModifications = oldModifiedRows.compare(newModifiedRows, result_names = ('Old Trial', 'Updated Trial'))

    # Display results
    print("\nNew added rows include:\n")
    print(addedRows)
    print("\nNew deleted rows include:\n")
    print(deletedRows)
    print("\nModified rows include:\n")
    print(comparedModifications)

    # Determine the date and create file name
    now = datetime.now()
    dateTime = now.strftime('%m%d%y')
    changeLogExport = 'BMO_ClinicalTrialChanges_'+ dateTime + '.xlsx'

    # Define function to write to excel for changes
    def excelCreator(df_list, sheet_list, file_name):
        writer = pd.ExcelWriter(file_name, engine = 'xlsxwriter')   
        for dataframe, sheet in zip(df_list, sheet_list):
            dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
        writer.close()

    # Define list of dataframes and sheets
    excelDFs = [addedRows, deletedRows, comparedModifications, currentDataFrame]
    excelSheets = ['Added Trials','Deleted Trials','Modified Trials', 'All Pulled Trials']    

    # Run Excel Creator Function
    excelCreator(excelDFs, excelSheets, changeLogExport)

    print('\n\nSuccessfully generated .xlsx file flagging changes to tracked trials...')
    print('\nChange log file saved as ' + changeLogExport)

else:
    # Alerting user that no historic file was found
    print('No historic clinical trials data is recognized. We are iniitializing a new file for tracking.')
    print('\n\nInitialized file includes the following clinical trials data.')

    # Saving new data frame as a csv
    currentDataFrame.to_csv("oldClinicalTrialsData.csv")

    # Print the DataFrame
    print(currentDataFrame)
    