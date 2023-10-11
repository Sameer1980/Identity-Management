from elasticsearch7 import Elasticsearch
from elasticsearch7 import Elasticsearch, helpers
import yaml
import elasticsearch7
import datetime
from datetime import datetime
from dateutil import tz
from copy import deepcopy
import logging
import os


FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'
logging.basicConfig(filename = "/home/itlaked/git-repos/case-mgmt-kibana/logs/elk_transformation_15m_{}.log",format=FORMAT, level=logging.INFO)
logging.info("*"*60)

def extract_transform_load():
    # Parameter info
    credentials = yaml.safe_load(open(r'/home/itlaked/git-repos/case-mgmt-kibana/conf/conne.yaml'))

    #CONNECTION POOL
    logging.info("Establishing Elasticsearch Connection")
    env = 'dev'
    es = Elasticsearch( hosts = credentials['elasticsearch-%s' % env]['hosts'],
                        port = credentials['elasticsearch-%s' % env]['port'],
                        http_auth = (credentials['elasticsearch-%s' % env]['username'],credentials['elasticsearch-%s' % env]['password']),
                        http_compress = True,
                        use_ssl = True,
                        verify_certs = True,
                        ca_certs = credentials['elasticsearch-%s' % env]['ca_certs_path'])
    #print(es.info())

    #last 15 minutes from staging index
    logging.info("Scanning last 15 minutes data from elasticsearch staging index")
    output = elasticsearch7.helpers.scan( es,index=credentials['elasticsearch-%s' % env]['stg_index_name'],preserve_order=True, doc_type="_doc",
        size=1000,query={"query":{"range": { "timestamp": {"gte": "now-15m","lte": "now"}}}})

    #Transformation part
    logging.info("Data Transformation is in progress")
    actions = []
    for record in output:
        #print(record)
        if 'value' in record['_source']:
        #value.message.date
            if 'message' in record['_source']['value'] and 'date' in record['_source']['value']['message']:
                record['_source']['value']['message']['date'] = convert_est_to_utc(record['_source']['value']['message']['date'])

            #value.messageInformation.effectiveDate
            if 'messageInformation' in record['_source']['value'] and 'effectiveDate' in record['_source']['value']['messageInformation']:
                record['_source']['value']['messageInformation']['effectiveDate'] = convert_est_to_utc(record['_source']['value']['messageInformation']['effectiveDate'])

            #requestID
            if 'audit' in record['_source']['value'] :
                record['_source']['value']['audit']['requestID']=record['_source']['value']['audit']['requestID'].split('_')[-1]

            #value.admin.activityAdmin.createdOn
            if 'admin' in record['_source']['value']:
                if 'activityAdmin' in record['_source']['value']['admin']:
                    if 'createdOn' in record['_source']['value']['admin']['activityAdmin']:
                        record['_source']['value']['admin']['activityAdmin']['createdOn'] = convert_est_to_utc(record['_source']['value']['admin']['activityAdmin']['createdOn'])
                    #value.admin.activityAdmin.modifiedOn
                    if 'modifiedOn' in record['_source']['value']['admin']['activityAdmin']:
                        record['_source']['value']['admin']['activityAdmin']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['admin']['activityAdmin']['modifiedOn'])
                    #value.admin.activityAssignment.createdOn
                    if 'activityAssignment' in record['_source']['value']['admin']:
                        if 'createdOn' in record['_source']['value']['admin']['activityAssignment']:
                            record['_source']['value']['admin']['activityAssignment']['createdOn'] = convert_est_to_utc(record['_source']['value']['admin']['activityAssignment']['createdOn'])
                        #value.admin.activityAssignment.modifiedOn
                        if 'modifiedOn' in record['_source']['value']['admin']['activityAssignment']:
                            record['_source']['value']['admin']['activityAssignment']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['admin']['activityAssignment']['modifiedOn'])
                    #value.admin.functionAdmin.createdOn
                   if 'functionAdmin' in record['_source']['value']['admin']:
                        if 'createdOn' in record['_source']['value']['admin']['functionAdmin']:
                            record['_source']['value']['admin']['functionAdmin']['createdOn'] = convert_est_to_utc(record['_source']['value']['admin']['functionAdmin']['createdOn'])
                        #value.admin.functionAdmin.modifiedOn
                        if 'modifiedOn' in record['_source']['value']['admin']['functionAdmin']:
                            record['_source']['value']['admin']['functionAdmin']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['admin']['functionAdmin']['modifiedOn'])
                    #value.admin.podAdmin.createdOn
                    if 'podAdmin' in record['_source']['value']['admin']:
                        if 'createdOn' in record['_source']['value']['admin']['podAdmin']:
                            record['_source']['value']['admin']['podAdmin']['createdOn'] = convert_est_to_utc(record['_source']['value']['admin']['podAdmin']['createdOn'])
                        #value.admin.podAdmin.modifiedOn
                        if 'modifiedOn' in record['_source']['value']['admin']['podAdmin']:
                            record['_source']['value']['admin']['podAdmin']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['admin']['podAdmin']['modifiedOn'])
            #caseID
            if 'case' in record['_source']['value']:
                record['_source']['value']['case']['caseID']=record['_source']['value']['case']['caseID'].split('_')[-1]
                #value.case.caseCompletedStatusTime
                if 'caseCompletedStatusTime' in record['_source']['value']['case']:
                    record['_source']['value']['case']['caseCompletedStatusTime'] = convert_est_to_utc(record['_source']['value']['case']['caseCompletedStatusTime'])
                #value.case.caseCompletionDate
                if 'caseCompletionDate' in record['_source']['value']['case']:
                    record['_source']['value']['case']['caseCompletionDate'] = convert_est_to_utc(record['_source']['value']['case']['caseCompletionDate'])
                #value.case.caseDueDate
                if 'caseDueDate' in record['_source']['value']['case']:
                    record['_source']['value']['case']['caseDueDate'] = convert_est_to_utc(record['_source']['value']['case']['caseDueDate'])
                #vaue.case.createdOn
                if 'createdOn' in record['_source']['value']['case']:
                    record['_source']['value']['case']['createdOn'] = convert_est_to_utc(record['_source']['value']['case']['createdOn'])
                #value.case.modifiedOn
                if 'modifiedOn' in record['_source']['value']['case']:
                    record['_source']['value']['case']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['case']['modifiedOn'])
                #value.case.pendingEndDate
                if 'pendingEndDate' in record['_source']['value']['case']:
                    record['_source']['value']['case']['pendingEndDate'] = convert_est_to_utc(record['_source']['value']['case']['pendingEndDate'])
                #value.case.requestSubmissionTime
                if 'requestSubmissionTime' in record['_source']['value']['case']:
                    record['_source']['value']['case']['requestSubmissionTime'] = convert_est_to_utc(record['_source']['value']['case']['requestSubmissionTime'])
                #value.case.pendingStartDate
                if 'pendingStartDate' in record['_source']['value']['case']:
                    record['_source']['value']['case']['pendingStartDate'] = convert_est_to_utc(record['_source']['value']['case']['pendingStartDate'])
                #Replace Null by zero for value.case.averageHandleTime
                if 'averageHandleTime' in record['_source']['value']['case']:
                    record['_source']['value']['case']['averageHandleTime']= replace_null_by_zero(record['_source']['value']['case']['averageHandleTime'])
                    #print(record['_source']['value']['case']['averageHandleTime'])

                #taskID
                if len(record['_source']['value']['case']['task']) > 0:
                    for j in range(0,len(record['_source']['value']['case']['task'])):
                        if record['_source']['value']['case']['task'][j]['taskID']!= 'NULL':
                            taskID = record['_source']['value']['case']['task'][j]['taskID']
                            if len(taskID.split("_"))==3:
                                record['_source']['value']['case']['task'][j]['taskID']=record['_source']['value']['case']['task'][j]['taskID'].split('_')[-2]
                            elif len(taskID.split("_"))==2:
                                record['_source']['value']['case']['task'][j]['taskID']=record['_source']['value']['case']['task'][j]['taskID'].split('_')[-1]

                        #value.case.task.assignedDate
                        if 'assignedDate' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['assignedDate']= convert_est_to_utc(record['_source']['value']['case']['task'][j]['assignedDate'])
                            #print("Assigned date : " , record['_source']['value']['case']['task'][j]['assignedDate'])
                        #value.case.task.createdOn
                        if 'createdOn' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['createdOn']= convert_est_to_utc(record['_source']['value']['case']['task'][j]['createdOn'])
                            #print(record['_source']['value']['case']['task'][j]['createdOn'])
                        #value.case.task.creationTime
                        if 'creationTime' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['creationTime']= convert_est_to_utc(record['_source']['value']['case']['task'][j]['creationTime'])
                            #print(record['_source']['value']['case']['task'][j]['creationTime'])
                        #value.case.task.modifiedOn
                        if 'modifiedOn' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['modifiedOn']= convert_est_to_utc(record['_source']['value']['case']['task'][j]['modifiedOn'])
                            #print(record['_source']['value']['case']['task'][j]['modifiedOn'])
                        #value.case.task.slaDeadline
                        if 'slaDeadline' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['slaDeadline']= convert_est_to_utc(record['_source']['value']['case']['task'][j]['slaDeadline'])
                            #print(record['_source']['value']['case']['task'][j]['slaDeadline'])

                        #Replace Null by zero for value.case.task.taskCompletionTime
                        if 'taskCompletionTime' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['taskCompletionTime']= replace_null_by_zero(record['_source']['value']['case']['task'][j]['taskCompletionTime'])
                            #print(record['_source']['value']['case']['task'][j]['taskCompletionTime'])

                        #Replace NULL by Zero for taskhandlingtime
                        if 'taskHandlingTime' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['taskHandlingTime']= replace_null_by_zero(record['_source']['value']['case']['task'][j]['taskHandlingTime'])
                            #print(record['_source']['value']['case']['task'][j]['taskHandlingTime'])

                        #Replace NULL by Zero for targethandlingtime
                        if 'targetHandlingTime' in record['_source']['value']['case']['task'][j]:
                            record['_source']['value']['case']['task'][j]['targetHandlingTime']= replace_null_by_zero(record['_source']['value']['case']['task'][j]['targetHandlingTime'])
                            #print(record['_source']['value']['case']['task'][j]['targetHandlingTime'])

                        #Derived field - Efficiency value
                        if record['_source']['value']['case']['task'][j]['targetHandlingTime']==0.0 :
                            record['_source']['value']['case']['task'][j]['Efficiencyvalue']=0.0
                        else:
                            record['_source']['value']['case']['task'][j]['Efficiencyvalue'] = float(record['_source']['value']['case']['task'][j]['taskHandlingTime'] )/ float(record['_source']['value']['case']['task'][j]['targetHandlingTime'])
                        #print(record['_source']['value']['case']['task'][j]['Efficiencyvalue'])

                        #Derived field - Tasktype
                        if record['_source']['value']['case']['task'][j]['taskStatus'] == 'Unassigned':
                            record['_source']['value']['case']['task'][j]['Tasktype'] = 'New'
                        elif record['_source']['value']['case']['task'][j]['taskStatus'] == 'NULL':
                            record['_source']['value']['case']['task'][j]['Tasktype'] = 'NULL'
                        else:
                            record['_source']['value']['case']['task'][j]['Tasktype'] = 'Total'

                        #Derived field - SLAtype
                        if record['_source']['value']['case']['task'][j]['slaMetFlag'] == 'true':
                            record['_source']['value']['case']['task'][j]['SLAtype'] = 'inSLA'
                        elif record['_source']['value']['case']['task'][j]['slaMetFlag'] == 'NULL':
                            record['_source']['value']['case']['task'][j]['SLAtype'] = 'NULL'
                        else:
                            record['_source']['value']['case']['task'][j]['SLAtype'] = 'outSLA'

            #Replace Null by zero for value.admin.activityAdmin.targetAvgHandelTime
            if 'admin' in record['_source']['value']:
               if 'activityAdmin' in record['_source']['value']['admin']:
                    if 'targetAvgHandelTime' in record['_source']['value']['admin']['activityAdmin']:
                        record['_source']['value']['admin']['activityAdmin']['targetAvgHandelTime'] = replace_null_by_zero(record['_source']['value']['admin']['activityAdmin']['targetAvgHandelTime'])
                        record['_source']['value']['admin']['activityAdmin']['targetAvgHandelTime'] = float(record['_source']['value']['admin']['activityAdmin']['targetAvgHandelTime'])
                        #print(record['_source']['value']['admin']['activityAdmin']['targetAvgHandelTime'])

            #value.audit.eventTMS
            if 'audit' in record['_source']['value']:
                if 'eventTMS' in record['_source']['value']['audit']:
                    record['_source']['value']['audit']['eventTMS'] = convert_est_to_utc(record['_source']['value']['audit']['eventTMS'])
                    #print(record['_source']['value']['audit']['eventTMS'])
            #value.caseRequest.modifiedOn
            if 'caseRequest' in record['_source']['value']:
                if 'modifiedOn' in record['_source']['value']['caseRequest']:
                    record['_source']['value']['caseRequest']['modifiedOn'] = convert_est_to_utc(record['_source']['value']['caseRequest']['modifiedOn'])
                    #print(record['_source']['value']['caseRequest']['modifiedOn'])
                #value.caseRequest.createdOn
                if 'createdOn' in record['_source']['value']['caseRequest']:
                    record['_source']['value']['caseRequest']['createdOn'] = convert_est_to_utc(record['_source']['value']['caseRequest']['createdOn'])
                    #print(record['_source']['value']['caseRequest']['createdOn'])

        actions.append(record['_source'])
    #print(len(actions))
    #print(actions)
    #Splitting of taskID
    newdata= []
    for data in actions:
        if 'value' in data and 'case' in data['value'] and 'task' in data['value']['case']:
            task = data['value']['case'].pop('task')
            for tasks in task:
                dict1 = deepcopy(data)
                dict1['value']['case'].update({"task": tasks})
                newdata.append(dict1)
        else:
            newdata.append(data)
    #print(newdata)
    logging.info("Data Transformation succsessful")
    #print(len(newdata))

    if len(newdata)==0:
        logging.info("No new records avaliable in staging index")

    #Loading of data into reporting index
    if newdata:
           try:
                helpers.bulk(es, newdata , index = credentials['elasticsearch-%s' % env]['rpt_index_name'] , chunk_size=1000, request_timeout = 200)
                logging.info("Count of the records sent into reporting index :- {}".format(len(newdata)))
           except Exception:
                    logging.info("No new records avaliable in staging index")


def convert_est_to_utc(est_date):
    from_zone=tz.gettz('US/Eastern')
    to_zone= tz.gettz('UTC')
    if len(est_date) == 20:
        format = "%Y-%m-%dT%H:%M:%SZ"
    elif len(est_date) == 19:
        format = "%Y-%m-%dT%H:%M:%S"
    elif len(est_date) == 21:
        format = "%Y-%m-%dT%H:%M:%S.%f"
    else:
        format = "%Y-%m-%dT%H:%M:%S.%fZ"
    utc_date = datetime.strptime(est_date, format).replace(tzinfo=from_zone).astimezone(to_zone).strftime(format)
    return(utc_date)

def replace_null_by_zero(value):
    if value == 'NULL' or  value == ' ':
        return 0.0
    else:
        return value

if __name__ == "__main__":
    extract_transform_load()
