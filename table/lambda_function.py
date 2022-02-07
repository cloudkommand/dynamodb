import boto3
import botocore
# import jsonschema
import json
import traceback

from extutil import remove_none_attributes, account_context, ExtensionHandler, ext, \
    current_epoch_time_usec_num, component_safe_name

eh = ExtensionHandler()

def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        account_number = account_context(context)['number']
        region = account_context(context)['region']
        eh.refresh()
        prev_state = event.get("prev_state")
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        cdef = event.get("component_def")
        cname = event.get("component_name")
        table_name = cdef.get("name") or component_safe_name(project_code, repo_id, cname, max_chars=255)
    
        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            eh.declare_pass_back_data(pass_back_data)
        elif event.get("op") == "upsert":
            eh.add_op("get_table")

        elif event.get("op") == "delete":
            eh.add_op("delete_table")

        get_table(table_name, cdef, region, prev_state)
        create_table(table_name, cdef, region)
        remove_tags()
        add_tags()
        set_ttl(table_name)
        delete_gsis(table_name)
        ensure_table_not_updating(table_name)
        add_gsis(table_name, cdef)
        ensure_table_not_updating(table_name)
        update_stream(table_name, cdef)
        ensure_table_not_updating(table_name)
        delete_table(table_name)
            
        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Uncovered Error", {"error": str(e)}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()

def gen_table_link(table_name, region):
    return f"https://console.aws.amazon.com/dynamodb/home?region={region}#tables:selected={table_name};tab=overview"

def format_tags(tags_dict):
    return [{"Key": k, "Value": v} for k,v in tags_dict.items()]

def get_tags(table_arn):
    dynamodb = boto3.client("dynamodb")

    tags = {}
    first = True
    cursor = None
    while first or cursor:
        params = remove_none_attributes({
            "ResourceArn": table_arn,
            "NextToken": cursor
        })

        tags_response = dynamodb.list_tags_of_resource(
            **params
        )

        tags.update({
            item["Key"]: item["Value"]
            for item in tags_response.get("Tags")
        })
        cursor = tags_response.get("NextToken")
        first = False

    return tags

@ext(handler=eh, op="get_table")
def get_table(table_name, table_info, region, prev_state):
    dynamodb = boto3.client("dynamodb")

    if prev_state and prev_state.get("props") and prev_state.get("props").get("name"):
        prev_table_name = prev_state.get("props").get("name")
        if table_name != prev_table_name:
            eh.perm_error("Cannot Change Table Name", progress=0)
            return None
    
    try:
        existing_table_info = dynamodb.describe_table(TableName=table_name).get("Table")
        eh.add_log("Found Table", {"table_name": table_name})
        
        #################################################
        # Check for GSIS
        #################################################
        existing_gsis = existing_table_info.get("GlobalSecondaryIndexes") or []
        existing_gsi_dict = {item['IndexName']: item for item in existing_gsis}
        existing_gsi_names = existing_gsi_dict.keys()
        
        delete_gsis = list(set(existing_gsi_names) - set(table_info.get('gsis', {}).keys()))
        add_gsis = list(set(table_info.get('gsis', {}).keys()) - set(existing_gsi_names))

        create_params = gen_create_table_params(table_name, table_info)
        create_gsi_dict = {item['IndexName']: item for item in (create_params.get("GlobalSecondaryIndexes") or [])}

        for index_name, index_data in create_gsi_dict.items():
            if index_name in list(existing_gsi_names):
                create_key_data = {item['AttributeName']:item['KeyType'] for item in index_data['KeySchema']}
                existing_key_data = {item['AttributeName']:item['KeyType'] for item in existing_gsi_dict[index_name]['KeySchema']}
                print(create_key_data)
                print(existing_key_data)
                if create_key_data != existing_key_data:
                    delete_gsis.append(index_name)
                    add_gsis.append(index_name)
                    continue

                create_projection = index_data.get("Projection") 
                existing_projection = existing_gsi_dict[index_name].get("Projection")
                print(create_projection)
                print(existing_projection)
                if create_projection.get("NonKeyAttributes"):
                    create_projection['NonKeyAttributes'] = sorted(create_projection['NonKeyAttributes'])

                if existing_projection.get("NonKeyAttributes"):
                    existing_projection['NonKeyAttributes'] = sorted(existing_projection['NonKeyAttributes'])

                if create_projection != existing_projection:
                    delete_gsis.append(index_name)
                    add_gsis.append(index_name)
                    continue
        
        if delete_gsis:
            eh.add_op("delete_gsis", delete_gsis)
        if add_gsis:
            eh.add_op("add_gsis", add_gsis)

        #######################################
        # CHECK STREAM (CHECK TO SEE IF THIS WORKS)
        #######################################
        current_stream_spec = existing_table_info.get("StreamSpecification")
        create_stream_spec = create_params.get("StreamSpecification")
        print(f"current_stream_spec = {current_stream_spec}")
        print(f"create_stream_spec = {create_stream_spec}")
        if (not current_stream_spec) and (create_stream_spec.get("StreamEnabled") == False):
            pass
        elif current_stream_spec != create_stream_spec:
            eh.add_op("update_stream")

        #######################################
        # TTL
        #######################################
        ttl_info = dynamodb.describe_time_to_live(TableName=table_name).get("TimeToLiveDescription")
        existing_table_info.update(ttl_info)
        if table_info.get("ttl_attribute") and ((ttl_info.get("TimeToLiveStatus") in ['DISABLING', 'DISABLED']) or (ttl_info.get("AttributeName") != table_info.get("ttl_attribute"))):
            eh.add_op("set_ttl", {"add": True, "name":table_info.get("ttl_attribute")})
        elif not table_info.get("ttl_attribute") and ttl_info.get("TimeToLiveStatus") in ['ENABLING', 'ENABLED']:
            eh.add_op("set_ttl", {"add": False, "name": ttl_info.get("AttributeName")})

        table_arn = existing_table_info['TableArn']
        eh.add_props({
            "name": table_name,
            "arn": table_arn,
            "table_id": existing_table_info.get("TableId"),
            "stream_arn": existing_table_info.get("LatestStreamArn"),
            "stream_label": existing_table_info.get("LatestStreamLabel")
        })
        eh.add_links({"Table": gen_table_link(table_name, region)})

        ##########################################################
        # TAGS
        ##########################################################
        existing_tags = get_tags(eh.props['arn'])
        tags = table_info.get("tags", {})

        if tags != existing_tags:
            new_tags = {k:v for k,v in tags.items() if ((k not in existing_tags) or (v != existing_tags.get(k)))}
            old_tags = {k:v for k,v in existing_tags.items() if (k not in tags)}
            if new_tags:
                eh.add_op("add_tags", new_tags)
            if old_tags:
                eh.add_op("remove_tags", list(old_tags.keys()))


    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.add_log("Table Does Not Exist", {"table_name": table_name})
            eh.add_op("create_table")
            if table_info.get("ttl_attribute"):
                eh.add_op("set_ttl", {"add": True, "name": table_info.get("ttl_attribute")})

        else:
            eh.add_log("Get Table Failed", {"error": str(e)})
            eh.retry_error("Failed Get", {"Exception": str(e)})

@ext(handler=eh, op="ensure_table_not_updating")
def ensure_table_not_updating(table_name):
    dynamodb = boto3.client("dynamodb")

    try:
        existing_table_info = dynamodb.describe_table(TableName=table_name).get("Table")
        print(f"Check Table Updating")
        print(existing_table_info)
        if existing_table_info.get("TableStatus") != "ACTIVE":
            eh.add_log("Table Updating", {"table_name": table_name, "props": eh.props})
            eh.retry_error(str(current_epoch_time_usec_num()), progress=50, callback_sec=15)
        else:
            eh.add_log("Table Finished Updating", {"table_name": table_name, "props": eh.props})
            eh.add_props({
                "table_id": existing_table_info.get("TableId"),
                "stream_arn": existing_table_info.get("LatestStreamArn"),
                "stream_label": existing_table_info.get("LatestStreamLabel")
            })

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.perm_error("Table Does Not Exist Anymore", 30)
        else:
            eh.add_log("Check Table Updating Error", {"error": str(e)})
            eh.retry_error("Check Table Updating Error", progress=50)

@ext(handler=eh, op="delete_gsis", complete_op=False)
def delete_gsis(table_name):
    dynamodb = boto3.client("dynamodb")
    index_to_delete = eh.ops['delete_gsis'][0]

    print(f"index_to_delete = {index_to_delete}")
    try:
        dynamodb.update_table(
            TableName=table_name,
            GlobalSecondaryIndexUpdates = [{
                "Delete": {
                    "IndexName": index_to_delete
                }
            }]
        )
        eh.add_log(f"Removing GSI {index_to_delete}", {"table_name": table_name})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.perm_error("Table Does Not Exist Anymore", 30)
            return None
        else:
            eh.add_log("Delete GSI Error", {"error": str(e)})
            eh.retry_error("Update Stream Error", progress=50)
            return None

    if len(eh.ops['delete_gsis']) == 1:
        eh.complete_op("delete_gsis")
    else:
        eh.ops['delete_gsis'] = eh.ops['delete_gsis'][1:]

    eh.add_op("ensure_table_not_updating")
    ensure_table_not_updating(table_name)


@ext(handler=eh, op="add_gsis", complete_op=False)
def add_gsis(table_name, table_info):
    dynamodb = boto3.client("dynamodb")
    index_to_add = eh.ops['add_gsis'][0]
    this_gsi = table_info.get("gsis").get(index_to_add)

    attribute_definitions = gen_attribute_definitions(this_gsi.get("pkey_name"), this_gsi.get("pkey_type", "S"), this_gsi.get("skey_name"), this_gsi.get("skey_type", "S"))
    attribute_definitions_revised = [{"AttributeName": k, "AttributeType": v} for k,v in attribute_definitions.items()]


    print(f"this_gsi = {this_gsi}")
    try:
        response = dynamodb.update_table(
            TableName=table_name,
            AttributeDefinitions=attribute_definitions_revised,
            GlobalSecondaryIndexUpdates = [{
                "Create": gen_gsi_description(index_to_add, this_gsi)
            }]
        )
        eh.add_log(f"Added GSI {index_to_add}", response)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.perm_error("Table Does Not Exist Anymore", 30)
            return None
        else:
            eh.add_log("Delete GSI Error", {"error": str(e)})
            eh.retry_error("Update Stream Error", progress=50)
            return None

    if len(eh.ops['add_gsis']) == 1:
        eh.complete_op("add_gsis")
    else:
        eh.ops['add_gsis'] = eh.ops['add_gsis'][1:]

    eh.add_op("ensure_table_not_updating")
    ensure_table_not_updating(table_name)


@ext(handler=eh, op="update_stream")
def update_stream(table_name, table_info):
    dynamodb = boto3.client("dynamodb")

    stream_spec = gen_create_table_params(table_name, table_info)['StreamSpecification']
    print(f"stream_spec = {stream_spec}")
    try:
        dynamodb.update_table(
            TableName=table_name,
            StreamSpecification = stream_spec
        )
        eh.add_log("Update Stream", {"stream_spec": stream_spec})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.perm_error("Table Does Not Exist Anymore", 30)
        else:
            eh.add_log("Update Stream Error", {"error": str(e)})
            eh.retry_error("Update Stream Error", progress=50)

    eh.add_op("ensure_table_not_updating")
    # ensure_table_not_updating(table_name)
    

@ext(handler=eh, op="set_ttl")
def set_ttl(table_name):
    dynamodb = boto3.client("dynamodb")
    
    try:
        response = dynamodb.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification=remove_none_attributes({
                "Enabled": eh.ops['set_ttl']['add'],
                "AttributeName": eh.ops['set_ttl']['name']
            })
        )
        eh.add_log("TTL Set", {"enabled": bool(eh.ops['set_ttl']), "attribute": eh.ops['set_ttl']})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.add_log("Table Busy", {"error": str(e)}, is_error=True)
            eh.retry_error("Set TTL", 10)
        else:
            print(str(e))
            eh.add_log("Unexpected Set TTL Error", {"error": str(e)}, is_error=True)
            eh.retry_error("Unexpected Set TTL Error", 10)

@ext(handler=eh, op="add_tags")
def add_tags():
    dynamodb = boto3.client("dynamodb")

    formatted_tags = format_tags(eh.ops['add_tags'])

    try:
        response = dynamodb.tag_resource(
            ResourceArn=eh.props['arn'],
            Tags=formatted_tags
        )
        eh.add_log("Tags Added", {"tags": formatted_tags})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.retry_error("Tag Limit", 50)
        

@ext(handler=eh, op="remove_tags")
def remove_tags():
    dynamodb = boto3.client("dynamodb")

    try:
        response = dynamodb.untag_resource(
            ResourceArn=eh.props['arn'],
            TagKeys=eh.ops['remove_tags']
        )
        eh.add_log("Tags Removed", {"tags": eh.ops['remove_tags']})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.add_log("Limit Hit While Untagging", {"error": str(e), "tags": eh.ops['remove_tags']}, is_error=True)
            eh.retry_error("Untag Limit", 50)
        elif e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.add_log("Error While Untagging", {"error": str(e), "tags": eh.ops['remove_tags']}, is_error=True)
            eh.retry_error("Untag Error", 50)

def gen_key_schema(pkey_name, skey_name):
    key_schema = [
        {
            "AttributeName": pkey_name,
            "KeyType": "HASH"
        }
    ]
    if skey_name:
        key_schema += [{
            "AttributeName": skey_name,
            "KeyType": "RANGE"
        }]

    return key_schema

def gen_gsi_description(gsi_name, gsi_description):
    return remove_none_attributes({
        "IndexName": gsi_name,
        "KeySchema": [{
            "AttributeName": gsi_description['pkey_name'],
            "KeyType": "HASH"
        }] if not gsi_description.get("skey_name") else [{
            "AttributeName": gsi_description['pkey_name'],
            "KeyType": "HASH"
        },{
            "AttributeName": gsi_description['skey_name'],
            "KeyType": "RANGE"
        }],
        "Projection": remove_none_attributes({
            "ProjectionType": gsi_description.get("projection_type") or "ALL",
            "NonKeyAttributes": gsi_description.get("projection_attributes")
        }),
        "ProvisionedThroughput": remove_none_attributes({
            "ReadCapacityUnits": gsi_description.get("read_capacity_units"),
            "WriteCapacityUnits": gsi_description.get("write_capacity_units")
        }) or None
    })

def gen_attribute_definitions(pkey_name, pkey_type, skey_name, skey_type):
    attribute_definitions = {pkey_name: pkey_type}
    if skey_name:
        attribute_definitions[skey_name] = skey_type

    return attribute_definitions

def gen_create_table_params(table_name, table_info):
    pkey_name = table_info.get("pkey_name") or "pkey"
    pkey_type = table_info.get("pkey_type") or "S"
    skey_name = table_info.get("skey_name")
    skey_type = table_info.get("skey_type") or "S"
    gsis = table_info.get("gsis") or {}
    tags = format_tags(table_info.get("tags", {})) or None
    enable_stream = table_info.get("enable_stream") or bool(table_info.get("stream_type"))
    stream_type = table_info.get("stream_type") or "NEW_AND_OLD_IMAGES"
    sse_spec = None
    billing_mode = table_info.get("billing_mode") or "PAY_PER_REQUEST"

    if billing_mode == "PROVISIONED":
        provisioned_spec = {
            "ReadCapacityUnits": table_info.get("read_capacity_units"),
            "WriteCapacityUnits": table_info.get("write_capacity_units")
        }

    else:
        provisioned_spec = None

    if enable_stream:
        stream_spec = {
            "StreamEnabled": True,
            "StreamViewType": stream_type
        }

    else:
        stream_spec = {
            "StreamEnabled": False
        }

    attribute_definitions = gen_attribute_definitions(pkey_name, pkey_type, skey_name, skey_type)
    key_schema = gen_key_schema(pkey_name, skey_name)

    global_secondary_indexes = []
    for gsi_name, gsi_description in gsis.items():
        global_secondary_indexes.append(gen_gsi_description(gsi_name, gsi_description))

        attribute_definitions[gsi_description['pkey_name']] = gsi_description.get("pkey_type") or "S"
        if gsi_description.get("skey_name"):
            attribute_definitions[gsi_description['skey_name']] = gsi_description.get("skey_type") or "S"

    attribute_definitions_revised = [{"AttributeName": k, "AttributeType": v} for k,v in attribute_definitions.items()]

    params = remove_none_attributes({
        "AttributeDefinitions": attribute_definitions_revised,
        "TableName": table_name,
        "KeySchema": key_schema,
        "GlobalSecondaryIndexes": global_secondary_indexes or None,
        "BillingMode": billing_mode,
        "ProvisionedThroughput": provisioned_spec,
        "StreamSpecification": stream_spec,
        "SSESpecification": sse_spec,
        "Tags": tags
    })

    return params

@ext(handler=eh, op="create_table")
def create_table(table_name, table_info, region):
    dynamodb = boto3.client("dynamodb")

    params = gen_create_table_params(table_name, table_info)

    try:
        table_response = dynamodb.create_table(**params).get("TableDescription")
        eh.add_log("Table Created", table_response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            eh.add_log("Table Created but Get Failed", {"error": str(e)}, is_error=True)
            eh.perm_error("Table Created but Get Failed", 0)
        elif e.response['Error']['Code'] == "LimitExceededException":
            eh.add_log("DynamoDB Limit Hit", {"error": str(e)}, is_error=True)
            eh.retry_error("Limit Hit", 0)
        else:
            eh.add_log("Create Error", {"error": str(e)}, is_error=True)
            eh.retry_error("Wait to Delete", {"error": str(e)})
    
    eh.add_props({
        "name": table_name,
        "arn": table_response.get("TableArn"),
        "table_id": table_response.get("TableId"),
        "stream_arn": table_response.get("LatestStreamArn"),
        "stream_label": table_response.get("LatestStreamLabel")
    })

    eh.add_links({"Table": gen_table_link(table_name, region)})

@ext(handler=eh, op="delete_table")
def delete_table(table_name):
    dynamodb = boto3.client("dynamodb")

    try:
        response = dynamodb.delete_table(TableName=table_name)
        eh.add_log("Deleted Table", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            eh.add_log("Table Does Not Exist", {"table_name": table_name})
            eh.complete_op("delete_table")
        elif e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.add_log("Waiting to Delete", {"error": str(e)}, is_error=True)
            eh.retry_error("Wait to Delete")
        else:
            eh.retry_error(str(e))





    

