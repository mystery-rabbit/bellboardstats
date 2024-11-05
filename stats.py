#! /usr/bin/env python
#

"""
Python script to parse Bellboard QP stats.

Target: Leicestershire Ringers

So firstly - look on the "Features" page and you can see a Leading Ringers search option that accepts an 
"association" .. in which case this is your Leading QP Ringers for LDG 2023 - and so you can edit the URL
 accordingly to get 2022 etc.
https://bb.ringingworld.co.uk/leading-ringers.php?association_id=17&association=Leicester+Diocesan+Guild&year=2023&annual_totals&length=quarter

But.. that's not what you're presenting in your data so then i assumed it was QPs in leicestershire which 
if we note that a general search URL looks like this... 
https://bb.ringingworld.co.uk/search.php?region=leicestershire&year=2023&length=quarter

then we might guess that by combining the two URLs above we get:
https://bb.ringingworld.co.uk/leading-ringers.php?region=leicestershire&year=2023&annual_totals&length=quarter

which whilst it works is still not matching what you presented since you seem to be generating stats for 
"leicestershire ringers".

and that's the issue - I don't think you can do that easily since bellboard has no concept that matches 
your criteria of a "leicestershire" ringer - ie: the vast majority of Alistair's QPs were rung out of 
county and out of guild. 

Robert Response:
I should have been clearer in what I was looking for though you worked it out though by a process of elimination: 
I was looking for all quarters rung by LDG/Leicestershire ringers, wherever they rang them (when I did the analysis 
last year I thought this would help my ranking, but I was wrong).  The fundamental problem in what I wanted to do is 
that I don't have a list of all LDG/Leicestershire ringers, so I adopted a two-step process - I looked for leading 
ringers by association (as you did first) and then searched individually for each of thech  top 20 ringers to see how 
many they had rung in total.  I guess that is a hard searcriteria to code up….  Alistair inevitably jumped over me 
because he rings quite a few in Northamptonshire and goes on quarter peal trips.  

API Docs:
https://bb.ringingworld.co.uk/help/api.php
- query-type: application/xml will return xml

Steps: 
Get list of ringers by assoc:
https://bb.ringingworld.co.uk/leading-ringers.php?association_id=17&association=Leicester+Diocesan+Guild&year=2023&annual_totals&length=quarter
Then get the list by county
combine, and de-duplicate.
produce stats for county, guild and an XOR of both.

then query for personal totals:
https://bb.ringingworld.co.uk/search.php?year=2024&annual_totals&length=quarter&ringer=%22Moira+Johnson%22


"""
import requests
import xmltodict
import pandas
import time
from pprint import pprint

# -----------------------------------------------------------------------------

SLEEP = 0.2 # slows down the API Calls to ensure we don't abuse BellBoard. 5cps.
PAGESIZE=1000 # limits how many records are returned and allows us to check for overruns. BB supports 10k but doesn't set 'next' headers.
DEBUG=0 # limits run to DEBUG ringers
# -----------------------------------------------------------------------------


def fetchbbxml( url, ) -> dict :
    """
    Fetch a URL, return XML results as a dict - for use with specific Bell Board URLs that can return XML.
    See Bell Boards API pages.
    """
    time.sleep( SLEEP )
    response = requests.get(
        url = url,
        headers = {
            'Accept':'application/xml',  
            'Content-Type':'application/xml',  
            } 
    )
    if response.status_code != 200:
        print( f'Failed Status Code: {response.status_code} for url: {url}')
        return {}
    print(f"Fetched URL: {url}")

    return( xmltodict.parse(response.content) )

def ringerslist(years: dict) ->list:
    """
    returns a list of dicts; dicts contain name, and ldg_<years> ie: ldg_2023, ldg_2024, etc.
    intended to be imported into a pandas.DataFrame
    """
    prefixes = [ 'ldg','county']

    ldg_ringers = {} 
    # ldg_ringers = {
    #     'name': {
    #         '2001': {
    #             'ldg': [ 'idA', 'idB', 'idC', ],
    #             'county': [ 'idA', 'idC', 'idD' ],
    #         },
    #         '2002': ...
    #     } 
    # }

    for year in range( years['from'], years['to']+1 ):
        urls = {
            "ldg": f"https://bb.ringingworld.co.uk/export.php?association_id=17&year={year}&length=quarter&pagesize={PAGESIZE}", 
            "county": f"https://bb.ringingworld.co.uk/export.php?region=leicestershire&year={year}&length=quarter&pagesize={PAGESIZE}",
            }
        for prefix in prefixes:
            dict_performances = fetchbbxml( urls[prefix] )

            if len(dict_performances['performances']['performance']) == PAGESIZE:
                print( "WARNING: number of returned records is equal to max pagesize. Records may be missing")
            print(f"returned {len(dict_performances['performances']['performance'])} records")

            # extract list of ringers from the xml.
            for performance in dict_performances['performances']['performance']:
                for ringer in performance['ringers']['ringer']:
                    name = ringer['#text']
                    id = performance['@id']

                    if not ldg_ringers.get(name):
                        ldg_ringers[name] = {}
                    if not ldg_ringers[name].get(year):
                        ldg_ringers[name][year] = {}
                    if not ldg_ringers[name][year].get(prefix):
                        ldg_ringers[name][year][prefix] = []

                    if id in ldg_ringers[name][year]:
                        print( f"duplicate performance {id} for ringer {name}")
                    else:                
                        ldg_ringers[name][year][prefix].append(id)
    
    new_prefix = "xor"
    for n in ldg_ringers:
        for year in ldg_ringers[n]:
            tmp = []
            for prefix in ldg_ringers[n][year]:
                tmp.extend(ldg_ringers[n][year][prefix])
            ldg_ringers[n][year][new_prefix] = list(dict.fromkeys(tmp)) # de-duplicate list

    prefixes.append(new_prefix)

    ringerlist = []
    count = DEBUG
    for n in ldg_ringers:
        if DEBUG:
            if count <= 0:
                break
            count -= 1

        record = { "name": n }
        for year in range(years['from'], years['to'] +1 ):
            for prefix in prefixes:
                colname = f"{prefix}_{year}"
                if year in ldg_ringers[n] and prefix in ldg_ringers[n][year]:
                    record[colname] = len( ldg_ringers[n][year][prefix] )
                else:
                    record[colname] = 0
        ringerlist.append(record)

    return(ringerlist)

def ringerperformances(ringer: pandas.Series, years ) -> pandas.Series:
    """
    Returns a Pandas.series to be added as columns to the DF
    Fetches the Ringers full QP totals for the year ( any, all and no association :-) )
    intended to be used with DataFrame.apply()
    """
    for year in range( years['from'], years['to'] +1 ):

        ringerurl = f"https://bb.ringingworld.co.uk/export.php?annual_totals&length=quarter&ringer=%22{ringer.get('name').replace(' ', '+')}%22&year={year}&pagesize={PAGESIZE}"   

        print(f" Fetching ringer {ringer.get('name')} year {year}")        
        dict_performances = fetchbbxml( ringerurl )
        try:
            # will throw exception if no performances are returned.
            perfs = len(dict_performances['performances']['performance'])
        except:
            perfs = 0

        print(f" returned {perfs} records")
        if perfs == PAGESIZE:
            print( "WARNING: number of returned records is equal to max pagesize. Records may be missing")
        
        ringer[ f"all_{year}" ] = perfs
    return(ringer)

#------------------------------------------------------------------------------

def main():

    filename = "output.xlsx"
    years = { "from": 2020, "to": 2024 } 

    df_ringers = pandas.DataFrame(ringerslist(years))
    # index, name, ldg_2023, ldg_2024, ...

    df_ringers = df_ringers.apply( ringerperformances, axis=1, result_type='reduce', years=years  )
    # ..., all_2023, all_2024,

    df_ringers.to_excel( filename )

if __name__ == "__main__":
    exit( main() )
