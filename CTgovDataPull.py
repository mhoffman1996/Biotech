import requests
import pandas as pd

# Initial URL for the first API call
base_url = "https://clinicaltrials.gov/api/v2/studies"
params = {
    "query.term": "bitopertin",
    "pageSize": 100
}

# Initialize an empty list to store the data
data_list = []

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
            enrollment = study['protocolSection']['designModule'].get('enrollmentInfo',{}).get('count', 'Unknown')
            startDate = study['protocolSection']['statusModule'].get('startDateStruct', {}).get('date', 'Unknown Date')
            conditions = ', '.join(study['protocolSection']['conditionsModule'].get('conditions', ['No conditions listed']))
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

# Create a DataFrame from the list of dictionaries
df = pd.DataFrame(data_list)

# Optionally, save the DataFrame to a CSV file
df.to_csv("oldClinicalTrialsData.csv", index=False)

# Print the DataFrame
df