import time
import requests
import tableauserverclient as TSC
import datetime
import pytz
import json
dependencies_dict = {}
utc=pytz.UTC
#what constitutes "done" for upstream dependencies?
acceptable_margin = datetime.timedelta(minutes=5)
a=1
precedent_list = []
#comma separated list of flows to refresh
flow_list = ['Flow1', 'block until done']
flow_objects = []
ts_url = <https url for server>
flow_string = "["
for i in flow_list:
    flow_string += '"' + i + '",'
flow_string = flow_string[:-1] + "]"

#this query will get all flows in your list and compute their upstream dependencies
mdapi_query = '''query flows {
      flows (filter: {nameWithin : ''' + flow_string + '''}){
    name
    upstreamDatasources {
      luid
    }
  }
}'''


tableau_auth = TSC.PersonalAccessTokenAuth(<PAT name>, <PAT Value>, <PAT Site>)
server = TSC.Server(ts_url, use_server_version = True)

#sign in and run MDAPI query to compute dependencies
with server.auth.sign_in(tableau_auth):
    auth_token = server.auth_token
    all_ds, pagination_item = server.datasources.get()
    all_flows, pagination_item = server.flows.get()
    for flow in all_flows:
        if flow.name in flow_list:
            flow_objects.append(flow)
    auth_headers = auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': auth_token}
    metadata_query = requests.post(ts_url + '/api/metadata/graphql', headers = auth_headers, verify=True, json = {"query": mdapi_query})
    mdapi_result = json.loads(metadata_query.text)
    for flow in mdapi_result['data']['flows']:
        dependencies = []
        for i in flow['upstreamDatasources']:
            dependencies.append(i['luid'])
        print('"' + flow['name'] +'"' + ' is waiting on ' + str(dependencies))
        #create dictionary of flows and their dependencies
        dependencies_dict[flow['name']] = dependencies

#this will keep running forever.  every 10 seconds it will check upstream dependencies
#if all dependencies have refreshed within the acceptable margin defined above, it will run the next step      
while a ==1:
    now=utc.localize(datetime.datetime.now())
    with server.auth.sign_in(tableau_auth):
        print('Checking again')
        refreshed_flows = []
        for flow in dependencies_dict:
            refreshed_flows.clear()

            time_since_refresh = []
            for i in dependencies_dict[flow]:
                time_delta = (now - (server.datasources.get_by_id(i).updated_at - datetime.timedelta(hours=8)))
                time_since_refresh.append(time_delta)
            #check all upstream refreshes vs acceptable margin
            refresh = (all(i < acceptable_margin for i in time_since_refresh))
            #if its refresh time, refresh the viz
            if refresh:
                refreshed_flows.append(flow)
                for i in flow_objects:
                    if i.name == flow:
                        server.flows.refresh(i)

                print('refreshing "' + flow +'"')
                
        #remove any refreshed flows from the list
        for i in refreshed_flows:
            dependencies_dict.pop(i)
    print('still waiting for' +str(dependencies_dict))
    #wait 10 seconds then try again from the top of the while loop
    time.sleep(10)