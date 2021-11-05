import json, requests, time, sys, linecache
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

#--------Main Function to Grab Distribution IDs----------
def listDistributions(qtoken, baseUrl1):

    baseUrl1 = baseUrl1
    headers = {
    "x-api-token": qtoken,
    "Content-Type": 'application/json'
     }
    nextPage = "init"

    while nextPage:
        try:
            response = requests.get(baseUrl1, headers=headers)
            responseJson = response.json()
            responseArray1 = responseJson['result']['elements']
            nextPage = responseJson['result']['nextPage']
            baseUrl1 = nextPage  
        except:
            pass

        with ThreadPoolExecutor(max_workers=15) as executor:
            for i in range(0, len(responseArray1)):
                executor.submit(getDistributions, headers, responseArray1, i)
               
#-------Function to grab distribution stats---------
def getDistributions(headers, responseArray1, i):
    nextPage2 = "init"
    id = responseArray1[i]['id']
    if responseArray1[i]['stats']['sent'] > 0:
        baseUrl2 = f"https://au1.qualtrics.com/API/v3/distributions/{id}/history"
        while(baseUrl2):
            try: 
                response = requests.get(baseUrl2, headers=headers)
                responseJson = response.json()
                responseArray2 = responseJson['result']['elements']
                baseUrl2 = responseJson['result']['nextPage']
                responseArrayObj = pd.DataFrame(responseArray2)
                responseArrayObj.to_csv('distributions.csv', mode='a', encoding='utf-8')
            except:
                pass


def main():
    start_time = time.time()
    qtoken = "" #replace with your own Qualtrics token
    surveyId = "SV_3BOj1DF1WbnOFAp"
    baseUrl1 = f"https://au1.qualtrics.com/API/v3/distributions?surveyId={surveyId}&distributionRequestType=Invite"
    listDistributions(qtoken, baseUrl1)
    print("---Execution time: %s seconds ---" % (time.time() - start_time))

if __name__== "__main__":
    main()

