import requests, gzip
from flask import escape
from json import loads
from uuid import uuid4
from sys import exc_info
from dateutil.parser import parse as dateutil_parser_parse
from google.cloud import bigquery

# Test: curl -X POST "https://us-central1-deployment-test-vms.cloudfunctions.net/test-mta" -H "Content-Type:application/tlsrpt+json" --data '{"organization-name":"ludemo.net","date-range":{"start-datetime":"2020-04-13T00:10:00Z","end-datetime":"2020-04-13T00:00:00Z"},"contact-info":"tlsrpt-feedback@socketlabs.com","report-id":"13B8FEDF21D3DBD6997654968E563122","policies":[{"policy":{"policy-type":"sts","policy-string":["version:STSv1","mode:enforce","mx:mta-sts.test.mailflow.ovh","max_age:86401"],"policy-domain":"test.mailflow.ovh","mx-host":["mta-sts.test.mailflow.ovh"]},"summary":{"total-successful-session-count":12,"total-failure-session-count":0}},{"policy":{"policy-type":"sts","policy-string":["version:STSv1","mode:enforce","max_age:86401"],"policy-domain":"test.mailflow.ovh","mx-host":[]},"summary":{"total-successful-session-count":0,"total-failure-session-count":2},"failure-details":[{"result-type":"certificate-host-mismatch","sending-mta-ip":"142.0.177.133","receiving-mx-host-name":"mta-sts.test.mailflow.ovh","receiving-ip":"18.203.131.132","failed-session-count":2}]}]}'

def iso8601_to_date(iso8601_str):
    return dateutil_parser_parse(iso8601_str).strftime('%Y-%m-%d %H:%M:%S.%f')

def parse_mta_sts_report(data):

    errors = []

    report = {}
    report['report_id'] = None
    try:
        report['report_id'] = data['report-id']
        report['organization_name'] = data['organization-name']
        report['date_range'] = str(data['date-range'])
        report['date_range_start_datetime'] = iso8601_to_date(data['date-range']['start-datetime'])
        report['date_range_end_datetime'] = iso8601_to_date(data['date-range']['end-datetime'])
        report['contact_info'] = data['contact-info']
    except Exception as e:
        errors.append({'report_id':report['report_id'], 'error':str(e), 'report':str(data)})
        print("Unexpected error for report:", e)

    policies = []
    failure_details = []

    for policies_entry in data['policies']:
        policy = {}
        try:
            policy['policy_id'] = str(uuid4())
            policy['report_id'] = report['report_id']

            policy_entry = policies_entry['policy']
            summary = policies_entry['summary']

            policy['type'] = policy_entry['policy-type']

            policy_strings = {}
            policy['domain'] = policy_entry['policy-domain']
            if 'policy-string' in policy_entry:
                try:
                    for policy_string in policy_entry['policy-string']:
                        key, value = policy_string.split(':')
                        key=key.strip()
                        value=value.strip()
                        policy_strings[key] = value
                except Exception as e:
                    print("Unexpected error for report:", e, policy_string)
            policy['version'] = policy_strings['version'] if 'version' in policy_strings else None
            policy['mode'] = policy_strings['mode'] if 'mode' in policy_strings else None
            policy['mx'] = policy_strings['mx'] if 'mx' in policy_strings else None
            policy['max_age'] = policy_strings['max_age'] if 'max_age' in policy_strings else None
            policy['policy_strings_joined'] = ','.join(policy_entry['policy-string']) if 'policy-string' in policy_entry else None
            policy['mx_hosts'] = ','.join(policy_entry['mx-host']) if 'mx-host' in policy_entry else None
            policy['summary_successful_session'] = summary['total-successful-session-count']
            policy['summary_failure_session'] = summary['total-failure-session-count']

            policies.append(policy)

            # failures
            try:
                if 'failure-details' in policies_entry:
                    for failure_detail in policies_entry['failure-details']:
                        failure_details_entry = {}

                        failure_details_entry['report_id'] = policy['report_id']
                        failure_details_entry['policy_id'] = policy['policy_id']

                        failure_details_entry['result_type'] = failure_detail['result-type']
                        failure_details_entry['sending_mta_ip'] = failure_detail['sending-mta-ip']
                        failure_details_entry['receiving_mx_hostname'] = failure_detail['receiving-mx-hostname'] if 'receiving-mx-hostname' in failure_detail else None
                        failure_details_entry['receiving_ip'] = failure_detail['receiving-ip']
                        failure_details_entry['failed_session_count'] = failure_detail['failed-session-count']

                        failure_details.append(failure_details_entry)
            except Exception as e:
                print("Unexpected error for failure_details:", e)
                errors.append({'report_id':report['report_id'], 'error':str(e), 'report':str(data)})
        except Exception as e:
            print("Unexpected error for policy:", e)
            errors.append({'report_id':report['report_id'], 'error':str(e), 'report':str(data)})

    return report, policies, failure_details, errors

def get_json_request_data(request):
    print(request.content_type)
    tlsrpt_json = None
    #TODO check size of request.data, if larger than cloud function memory, skip
    if request.content_type == 'application/tlsrpt+gzip':
        uncompressed_data = gzip.decompress(request.data)
        print(uncompressed_data)
        tlsrpt_json = loads(uncompressed_data)
    elif request.content_type == 'application/tlsrpt+json':
        tlsrpt_json = request.get_json(silent=True)
    else:
        print("Unsupported content-type {}".format(request.content_type))
        tlsrpt_json = None
    print(tlsrpt_json)
    return tlsrpt_json

def store_in_bigquery(client, table_id, data_list):
    if len(data_list)>0:
        print(table_id)
        table = client.get_table(table_id)
        errors = client.insert_rows(table, data_list)
        print(errors)

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    print('mimetype:{}'.format(request.mimetype))
    print('content_type:{}'.format(request.content_type))
    print('method:{}'.format(request.method))
    print('query_string:{}'.format(request.query_string))
    print('referrer:{}'.format(request.referrer))
    request_json = request.get_json(silent=True)
    print('request_json:{}'.format(request_json))
    request_args = request.args
    print('request_args:{}'.format(request_args))
    print('data:{}'.format(request.data))
    
    print("Getting report data...")
    tlsrpt_json = get_json_request_data(request)
    if tlsrpt_json:
        print("Parsing report data...")
        report, policies, failure_details, errors = parse_mta_sts_report(tlsrpt_json)

        print("Parsed data:")
        print(report)
        print(policies)
        print(failure_details)
        print(errors)

        print("Storing report in bigquery...")
        client = bigquery.Client()
        store_in_bigquery(client, 'deployment-test-vms.MTA_STS_reports.MTA_STS_Report', [report])
        store_in_bigquery(client, 'deployment-test-vms.MTA_STS_reports.MTA_STS_Policies', policies)
        store_in_bigquery(client, 'deployment-test-vms.MTA_STS_reports.MTA_STS_Failures', failure_details)
        store_in_bigquery(client, 'deployment-test-vms.MTA_STS_reports.MTA_STS_Errors', errors)

        
        return ('Ok', 200, {})
    else:
        return ('Unsupported content_type, please retry'.format(request.content_type), 400, {})
