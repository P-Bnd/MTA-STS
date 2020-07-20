from uuid import uuid4
from sys import exc_info

from dateutil.parser import parse as dateutil_parser_parse

def iso8601_to_date(iso8601_str):
    return dateutil_parser_parse(iso8601_str).strftime('%Y-%m-%d %H:%M:%S.%f')

def parse_mta_sts_report(data):

    report = {}
    try:
        report['report_id'] = data['report-id']
        report['organization_name'] = data['organization-name']
        report['date_range'] = str(data['date-range'])
        report['date_range_start_datetime'] = iso8601_to_date(data['date-range']['start-datetime'])
        report['date_range_end_datetime'] = iso8601_to_date(data['date-range']['end-datetime'])
        report['contact_info'] = data['contact-info']
    except Exception as e:
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
        except Exception as e:
            print("Unexpected error for policy:", e)

    return report, policies, failure_details

def cloud_transfer(request):

    from google.cloud import bigquery

    # TODO(developer): Construct a BigQuery client object.
    client = bigquery.Client()

    #rows_to_insert = data_load
    #rows_to_insert = {"organization-name":"socketlabs.com","date-range":{"start-datetime":"2020-02-27T00:00:00Z","end-datetime":"2020-02-28T00:00:00Z"},"contact-info":"tlsrpt-feedback@socketlabs.com","report-id":"13B8FEDF21D3DBD6997654968E563122","policies":[{"policy":{"policy-type":"sts","policy-string":["version: STSv1","mode: enforce","mx: mta-sts.test.mailflow.ovh","max_age: 86401"],"policy-domain":"test.mailflow.ovh","mx-host":["mta-sts.test.mailflow.ovh"]},"summary":{"total-successful-session-count":12,"total-failure-session-count":0}},{"policy":{"policy-type":"sts","policy-string":["version: STSv1","mode: enforce","mx: mta-sts.test.mailflow.ovh","max_age: 86401"],"policy-domain":"test.mailflow.ovh","mx-host":[]},"summary":{"total-successful-session-count":0,"total-failure-session-count":2},"failure-details":[{"result-type":"certificate-host-mismatch","sending-mta-ip":"142.0.177.133","receiving-mx-host-name":"mta-sts.test.mailflow.ovh","receiving-ip":"18.203.131.132","failed-session-count":2}]}]}
    rows_to_insert = {"organization-name":"ludemo.net","date-range":{"start-datetime":"2020-04-13T00:10:00Z","end-datetime":"2020-04-13T00:00:00Z"},"contact-info":"tlsrpt-feedback@socketlabs.com","report-id":"13B8FEDF21D3DBD6997654968E563122","policies":[{"policy":{"policy-type":"sts","policy-string":["version:STSv1","mode:enforce","mx:mta-sts.test.mailflow.ovh","max_age:86401"],"policy-domain":"test.mailflow.ovh","mx-host":["mta-sts.test.mailflow.ovh"]},"summary":{"total-successful-session-count":12,"total-failure-session-count":0}},{"policy":{"policy-type":"sts","policy-string":["version:STSv1","mode:enforce","max_age:86401"],"policy-domain":"test.mailflow.ovh","mx-host":[]},"summary":{"total-successful-session-count":0,"total-failure-session-count":2},"failure-details":[{"result-type":"certificate-host-mismatch","sending-mta-ip":"142.0.177.133","receiving-mx-host-name":"mta-sts.test.mailflow.ovh","receiving-ip":"18.203.131.132","failed-session-count":2}]}]}
    #rows_to_insert = {"organization-name":"Google Inc.","date-range":{"start-datetime":"2020-02-06T00:00:00Z","end-datetime":"2020-02-06T23:59:59Z"},"contact-info":"smtp-tls-reporting@google.com","report-id":"2020-02-06T00:00:00Z_test.mailflow.ovh","policies":[{"policy":{"policy-type":"no-policy-found","policy-domain":"test.mailflow.ovh"},"summary":{"total-successful-session-count":1,"total-failure-session-count":0}},{"policy":{"policy-type":"sts","policy-string":["version: STSv1","mode: testing","max_age: 86401","mx: postfix1.test.mailflow.ovh","mx: test.maillfow.ovh","mx: redirect.ovh.net"],"policy-domain":"test.mailflow.ovh"},"summary":{"total-successful-session-count":0,"total-failure-session-count":3},"failure-details":[{"result-type":"validation-failure","sending-mta-ip":"209.85.219.43","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":1},{"result-type":"validation-failure","sending-mta-ip":"209.85.167.193","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":1},{"result-type":"validation-failure","sending-mta-ip":"209.85.210.68","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":1}]},{"policy":{"policy-type":"sts","policy-string":["version: STSv1","mode: testing","max_age: 86401","mx: postfix1.test.maillfow.ovh","mx: redirect.ovh.net","mx: *.mailflow.ovh"],"policy-domain":"test.mailflow.ovh"},"summary":{"total-successful-session-count":0,"total-failure-session-count":10},"failure-details":[{"result-type":"validation-failure","sending-mta-ip":"209.85.167.193","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":1},{"result-type":"validation-failure","sending-mta-ip":"209.85.210.65","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":1},{"result-type":"validation-failure","sending-mta-ip":"209.85.167.196","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":4},{"result-type":"validation-failure","sending-mta-ip":"209.85.167.194","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":2},{"result-type":"validation-failure","sending-mta-ip":"209.85.167.195","receiving-ip":"18.203.131.132","receiving-mx-hostname":"postfix1.test.mailflow.ovh","failed-session-count":2}]}]}
    data=rows_to_insert

    report, policies, failure_details = parse_mta_sts_report(data)

    print(report)
    print(policies)
    print(failure_details)

    table_id = 'deployment-test-vms.MTA_STS_reports.MTA_STS_Report'
    print(table_id)
    table = client.get_table(table_id)
    errors = client.insert_rows(table, [report])
    print(errors)

    if len(policies)>0:
        table_id = 'deployment-test-vms.MTA_STS_reports.MTA_STS_Policies'
        print(table_id)
        table = client.get_table(table_id)
        errors = client.insert_rows(table, policies)
        print(errors)
    
    if len(failure_details)>0:
        table_id = 'deployment-test-vms.MTA_STS_reports.MTA_STS_Failures'
        print(table_id)
        table = client.get_table(table_id)
        errors = client.insert_rows(table, failure_details)
        print(errors)
    
    #errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
    #if errors == []:
    #    print("New rows have been added.")
