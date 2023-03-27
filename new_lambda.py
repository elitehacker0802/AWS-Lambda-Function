from datetime import datetime
import boto3
import csv

from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    
    #Set up resource objects for AWS S3 bucket and DynomoDB tables.
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('cost-report-ada-test')
    table = dynamodb.Table("status_mockup_table")
    price_lookup_table = dynamodb.Table('resource_cost_table')
    
    # Get cycle_id parameter from the event
    cycle_id = event.get('cycle_id')
    if not cycle_id:
        raise ValueError('cycle_id parameter is missing')
    
    # Set the file name
    filename = '{0}-{1}.csv'.format(cycle_id, datetime.strftime(datetime.now(), "%Y-%m-%d-%H-%M-%S"))

    # Retrieve completed_task_list
    response = table.scan(FilterExpression=Attr('cycle_id').eq(event['cycle_id']) & Attr('completed_task_list').ne([]))
    items = response['Items']

    # Query DynamoDB table for items with matching cycle_id and non-empty completed_task_list
    while 'LastEvaluatedKey' in response:
        response = table.scan(FilterExpression=Attr('cycle_id').eq(event['cycle_id']) & Attr('completed_task_list').ne([]), ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    # Initialize the values
    report = []
    workflow_total_cost = 0
    
    field = ['Completed_task_list', 'task_name', 'task_invocation_resource_type', 'task_invocation_resource_name', 'Instance_Type', 'Instance_Count', 'Cost_per_Instance', 'Cost_per_resource', 'Time_elapsed', 'Total_Cost_of_Step']
    time_rate = {
        'per hour': 1,
        'per minute': 60,
        'per millisecond': 1
    }    
    
    # Iterate over the items and extract data from the completed_task_list attribute
    for item in items:
        
        # Retrieve process infos from each completed task list.
        process_name = item.get('process_name', "")
        process_start_date_time = datetime.strptime(item['process_start_date_time'], "%Y-%m-%d %H:%M:%S")
        process_end_date_time = datetime.strptime(item['process_end_date_time'], "%Y-%m-%d %H:%M:%S")
        Workflow_Total_time_Elapsed = process_end_date_time - process_start_date_time
        
        #Write report
        report.append(["Process Name: " + process_name])  
        report.append(field)
        
        for idx, task in enumerate(item['completed_task_list']):

            # Retrieve task infos from each completed task.
            # Make sub_report regarding each task and merge all the sub_report.
            sub_report = []
            task_name = task['task_name']
            aws_service = task['task_invocation_resource_type']
            aws_resource = task['task_invocation_resource_name']
            instance_type = task.get('InstanceType', "")
            instance_count = int(task.get('InstanceCount', 0))
            step_function_state_transition = int(task.get('step_function_state_transition', 0))
            
            # Initialize the values
            cost_per_instance = 0
            cost_per_resource = 0
            total_cost = 0
            
            # Calulate the cost 
            if aws_service == "lambda_function":
                elapsed_time = int(task['lambda_billled_duration'])
                price_lookup_response = price_lookup_table.scan(FilterExpression=Attr('Service_Type').eq(aws_service))
                time_for_cost = price_lookup_response['Items'][0]['Time_for_Cost']
                cost_per_resource = price_lookup_response['Items'][0]['Cost']
                total_cost = float(cost_per_resource * elapsed_time)
                
            elif step_function_state_transition > 0:
                price_lookup_response = price_lookup_table.scan(FilterExpression=Attr('Service_Resource_Type').eq('function_transition'))
                time_for_cost = price_lookup_response['Items'][0]['Time_for_Cost']
                cost_per_resource = price_lookup_response['Items'][0]['Cost']
                elapsed_time = step_function_state_transition
                total_cost = float(cost_per_resource * elapsed_time)
            else :
                start_time = datetime.strptime(task['task_start_time'], "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(task['task_end_time'], "%Y-%m-%d %H:%M:%S")
                elapsed_time = float(((end_time - start_time).total_seconds())) / 3600
                price_lookup_response = price_lookup_table.scan(FilterExpression=Attr('Instance_Type').eq(instance_type))
                time_for_cost = price_lookup_response['Items'][0]['Time_for_Cost']
                cost_per_instance = price_lookup_response['Items'][0]['Cost']
                
                elapsed_time = elapsed_time * time_rate[time_for_cost]
                total_cost = float(cost_per_instance) * float(elapsed_time) * float(instance_count)
                    
            workflow_total_cost = float(workflow_total_cost + total_cost)
            
            # Add data to sub_report
            sub_report.append(idx)
            sub_report.append(task_name)
            sub_report.append(aws_service)
            sub_report.append(aws_resource)
            sub_report.append(instance_type)
            sub_report.append(instance_count)
            sub_report.append(cost_per_instance)
            sub_report.append(cost_per_resource)
            sub_report.append(elapsed_time)
            sub_report.append(total_cost)
            
            report.append(sub_report)
    
    # Add data to report
    report.append([])
    report.append(['', 'workflow_total_cost', '', '', '', '', '', '', '', workflow_total_cost])
    report.append([])
    report.append(['', 'process_start_date_time', process_start_date_time])
    report.append(['', 'process_end_date_time', process_end_date_time])
    report.append(['', 'Workflow_Total_time_Elapsed', Workflow_Total_time_Elapsed])
    
    with open('/tmp/report.csv', 'w', newline='') as f:
        write = csv.writer(f)
        write.writerows(report)

    bucket.upload_file('/tmp/report.csv', filename)
    
    return {
        'status': 'Okay'
    }