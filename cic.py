import requests, json, sys
from pub_client import publisherClient
import esgcet.update as up
import esgcet.activity_check as ac
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import gzip
import shutil
import urllib3
import argparse


def get_args():
    parser = argparse.ArgumentParser(description="CMIP6 Inconsistency Checker: Check for metadata errors and inconsistencies in the ESGF database.")

    parser.add_argument("--output-dir", dest="output_directory", required=True,
                        help="Full path to destination directory for json output files. Please use ending '/'.")
    parser.add_argument("--cmor-tables", dest="cmor_tables", required=True,
                        help="Full path to CMOR tables directory. Please use ending '/'.")
    parser.add_argument("--test", dest="test", default=False, action="store_true", help="Enable for a test run or dry run.")
    parser.add_argument("--email", dest="email", default=None, nargs="+",
                        help="Primary email(s) to send summary of data to. Default is to not sent an email summary.")
    parser.add_argument("--enable-email", dest="enable_email", default=False, action="store_true",
                        help="Enable emailing of summary of results to complete ESGF affiliate email list.")
    parser.add_argument("--enable-ac", dest="ac", default=False, action="store_true",
                        help="Enable activity check, expect longer time for output processing.")
    parser.add_argument("--fix-errors", dest="fix_errs", default=False, action="store_true",
                        help="Retract and update metadata records on the LLNL ESGF node to fix errors detected by CIC.")
    parser.add_argument("--enable-errata", dest="errata", default=False, action="store_true",
                        help="Enable checking of the Errata database, expect longer time for output processing.")
    parser.add_argument("--get-replica-holdings", dest="get_holdings", default=False, action="store_true",
                        help="Enable output of replica holdings for comparison with SQLite database paths.")

    return parser.parse_args()


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

args = get_args()
TEST = args.test
EMAIL_LIST = args.enable_email
FIX_ERRS = args.fix_errs
DO_AC = args.ac
ERRATA_CHECK = args.errata
PRIMARY_EMAIL = args.email
SAVE_REPLICA_HOLDINGS = args.get_holdings
NUM_RETR = 10000
ORIGINAL_ERR = "No original record:"
NOF_ERR = "Inconsistent number of files (esgf replica issue):"
NOF_ERR2 = "Inconsistent number of files (client issue):"
LATEST_ERR = "Original record not latest version:"
RETRACT_ERR = "Original record retracted:"
DUP_ERR = "Duplicate records:"
RR_ERR = "Replica retracted, original not retracted:"
AC_ERR = "Failed activity check:"
EC_ERR = "Failed experiment_id check:"
ERRATA = "Errata found:"
duplicates = []
INDEX_NODE = "esgf-node.llnl.gov"
CERT = "/p/user_pub/publish-queue/certs/certificate-file"
CMOR_PATH = args.cmor_tables
DIRECTORY = args.output_directory
if SAVE_REPLICA_HOLDINGS:
    instance_file = open(DIRECTORY + "have_replicas.txt", "w")
CMOR_JSON = json.load(open("{}CMIP6_CV.json".format(CMOR_PATH)))["CV"]


def save_to_list(instance):
    instance_file.write(instance + "\n")


def run_ac(input_rec):
    jobj = CMOR_JSON
    sid_dict = jobj["source_id"]

    src_id = input_rec['source_id'][0]
    act_id = input_rec['activity_drs'][0]

    if src_id not in sid_dict:
        return False

    rec = sid_dict[src_id]
    return act_id in rec["activity_participation"]


def run_ec(rec):
    cv_table = CMOR_JSON

    act_id = rec['activity_drs'][0]
    exp_id = rec['experiment_id'][0]

    if exp_id not in cv_table['experiment_id']:
        return False
    elif act_id not in cv_table['experiment_id'][exp_id]['activity_id'][0]:
        return False
    else:
        return True


def compare(r, original, attr):  # use to compare attributes, r is response and attr is value string
    if r[attr] == original[attr]:
        return True
    else:
        return False


def get_list(node="default"):
    if node == "default":
        url = "https://esgf-node.llnl.gov/esg-search/search?facets=institution_id&project=CMIP6&format=application%2fsolr%2bjson"
    elif node == "esgf-node.ipsl.upmc.fr":
        url = "https://esgf-node.ipsl.upmc.fr/esg-search/search?project=CMIP6&limit=0&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esgf-node.llnl.gov":
        url = "https://esgf-node.llnl.gov/esg-search/search?project=CMIP6&limit=0&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esgdata.gfdl.noaa.gov":
        url = "https://esgdata.gfdl.noaa.gov/esg-search/search?limit=0&project=CMIP6&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esgf.nci.org.au":
        url = "https://esgf.nci.org.au/esg-search/search?limit=0&project=CMIP6&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esgf-data.dkrz.de":
        url = "https://esgf-data.dkrz.de/esg-search/search?limit=0&project=CMIP6&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esg-dn1.nsc.liu.se":
        url = "https://esg-dn1.nsc.liu.se/esg-search/search?limit=0&project=CMIP6&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    elif node == "esgf-index4.ceda.ac.uk":
        url = "https://esgf-index4.ceda.ac.uk/esg-search/search?limit=0&facets=institution_id&distrib=false&replica=false&latest=true&format=application%2fsolr%2bjson"
    else:
        print("ERROR: Invalid node.")
        exit(1)

    resp = json.loads(requests.get(url).text)
    lst = []
    i = 0
    for inst in resp["facet_counts"]["facet_fields"]["institution_id"]:
        if i % 2 == 0:
            lst.append(inst)
        i += 1
    return lst


def get_nodes():
    retries = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=2,
                                                         status_forcelist=[429, 500, 502, 503, 504])
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    http = requests.Session()
    http.mount("http://", adapter)

    unreachable = False
    skipped = 0
    a = ["esgf-node.ipsl.upmc.fr", "esgf-node.llnl.gov", "esgf.nci.org.au", "esgf-data.dkrz.de", "esg-dn1.nsc.liu.se",
         "esgf-index4.ceda.ac.uk", "esgdata.gfdl.noaa.gov"]

    lst = []
    bad_lst = []
    global FIX_ERRS

    for x in a:
        print(x)
        if not "llnl" in x:
            if "ceda" in x:
                url = "http://{}/esg-search/search?limit=0&facets=index_node&format=application%2fsolr%2bjson"
            else:
                url = "http://{}/esg-search/search?project=CMIP6&limit=0&facets=index_node&format=application%2fsolr%2bjson"
            try:
                res = http.get(url.format(x), timeout=120)
            except:
                unreachable = True
                skipped += 1
                print("Data node unreachable.")
                bad_lst.append(x)
        if not unreachable:
            lst.append(x)

    if skipped > 0:
        FIX_ERRS = False
    if skipped > 2:
        print("ERROR: more than 2 data nodes unreachable. Exiting.")
        exit(1)
    else:
        warnings.append("WARNING: following data nodes unreachable: " + str(bad_lst) + ". This may impact results.")
        return lst


def get_batch(search_url, institution):
    if TEST:
        return {}, 0
    going = True
    seen = 0
    offset = 0
    togo = 0
    count = 0
    tally = 0
    batch = {}
    found = 0
    retries = requests.packages.urllib3.util.retry.Retry(total=3, backoff_factor=2,
                                                         status_forcelist=[429, 500, 502, 503, 504])
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    http = requests.Session()
    http.mount("http://", adapter)

    while going:

        found = 0
        try:  # put in a timeout for buffering instances
            print(".", end="", flush=True)
            resp = json.loads(http.get(search_url.format(NUM_RETR, offset, institution), timeout=60).text)
        except Exception as x:
            print("Error with load. Loaded " + str(count * NUM_RETR) + " results from " + institution)
            warning = "WARNING: Error with loading results from " + institution + ": " + str(
                seen) + " results loaded. May impact error checking."
            warnings.append(warning)
            skips.append(institution)
            break

        numfound = resp["response"]["numFound"]
        found = numfound
        # check if numfound = len response docs, does numfound remain consistent
        if numfound == 0:
            print("ERROR: No results loaded from " + institution + ". Possible network error: " + search_url)
            warning = "ERROR: No results loaded from " + institution + ". Possible network error: " + search_url
            warnings.append(warning)
            return {}, -1

        if togo == 0:

            if numfound > NUM_RETR:

                togo = numfound - NUM_RETR  # used to keep track of batches left
                offset = offset + NUM_RETR  # used to goto next

            else:

                going = False
                print("Loaded " + str(numfound) + " results from " + institution)

        else:

            togo = togo - NUM_RETR
            offset = offset + NUM_RETR

            if togo < 1:
                going = False
                print("Loaded " + str(numfound) + " results from " + institution)
        for n in resp["response"]["docs"]:
            if n["instance_id"] not in batch.keys():
                batch[n["instance_id"]] = []
                batch[n["instance_id"]].append(n)
            else:
                duplicate = True
                for rec in batch[n["instance_id"]]:
                    for key in n.keys():
                        if n[key] != rec[key]:
                            duplicate = False
                if duplicate:
                    tally += 1
                    duplicates.append(rec)
                else:
                    batch[n["instance_id"]].append(n)
            seen += 1
        count += 1
        """if count >= 1:  # use to decide how many records to retrieve (testing tool)
            if going:
                going = False
                print("Loaded 10k (temp max) results from " + institution)"""
    print("Done.")
    if found == 0:
        print("Error with load, " + str(found) + " found.")
    elif seen < found:
        print("Error. Only " + str(seen) + " results loaded out of " + str(found) + ".")
        warning = "ERROR collecting results from " + institution + ": Only " + str(
            seen) + " results loaded out of " + str(found) + "."
        warnings.append(warning)
    return batch, found


def flag(field, err, group):
    if field not in inconsistencies[err].keys():
        inconsistencies[err][field] = []
    inconsistencies[err][field].append(group)


def count_error(err, field):
    if field not in error_counts[err].keys():
        error_counts[err][field] = 0
    error_counts[err][field] += 1


def check_errata(pid):
    get_error = "http://errata.es-doc.org/1/resolve/simple-pid?datasets={}".format(pid)
    try:
        resp = json.loads(requests.get(get_error, timeout=30, verify=False).text)
    except:
        print("Could not reach errata site.", file=sys.stderr)
        return False
    try:
        errata = resp[next(iter(resp))]["hasErrata"]
    except:
        print("Errata site threw error.", file=sys.stderr)
        return False
    if errata:
        return True
    else:
        return False


def find_inconsistencies(batch, institution):
    if TEST:
        return None
    print("Finding inconsistencies for " + institution + "...")

    for instance in (batch.keys()):
        nof_err = False
        not_latest = False
        multiples = False
        group = batch[instance]
        original = None
        prev = group[0]
        nof_rec = None
        if len(group) == 1 and not prev['replica']:
            if prev['retracted'] or not prev['latest']:
                continue
        replica_retracted = False
        failed_ac = False
        failed_ec = False
        have_replica = False
        all_rt = True
        for member in group:
            if not member["retracted"]:
                all_rt = False
            if not member["replica"]:  # check if member is original record
                if original is None:
                    original = member
                elif original['retracted'] and not member['retracted']:
                    original = member
                elif not original['latest'] and member['latest']:
                    if not compare(original, member, 'data_node') and compare(original, member, 'version'):
                        not_latest = True
                    else:
                        original = member
                elif not member['retracted'] and member['latest']:
                    if compare(original, member, 'id') and not compare(original, member, '_timestamp'):
                        print("MULTIPLE ORIGINALS: " + instance)
                        multiples = True
                    else:
                        nope = False
                        for key in member.keys():
                            if member[key] != original[key]:
                                nope = True
                                if institution == 'E3SM-Project':
                                    E3SM_f.append(group)
                                else:
                                    multiples = True
                        if not nope:
                            flag(member['data_node'], DUP_ERR, group)
                            count_error(DUP_ERR, institution)
            else:
                if member['retracted']:
                    replica_retracted = True
                if 'llnl' in member['data_node']:
                    have_replica = True
            if DO_AC:
                if not run_ac(member):
                    failed_ac = True
                elif not run_ec(member):
                    failed_ec = True
            if not compare(member, prev, "number_of_files"):  # consistency check for number of files field
                nof_rec = member
                nof_err = True
            prev = member
        if ERRATA_CHECK:
            if not all_rt:
                errata = check_errata(instance)
                if errata:
                    print("Errata found with this dataset, retracting.")
                    flag(prev["data_node"], ERRATA, group)
                    count_error(ERRATA, institution)
        if original is None:  # if no original record, return as an inconsistency sorted by data node
            flag(prev["data_node"], ORIGINAL_ERR, group)
            count_error(ORIGINAL_ERR, institution)
        elif original['retracted']:
            flag(prev["data_node"], RETRACT_ERR, group)
            count_error(RETRACT_ERR, institution)
        elif not original['retracted'] and replica_retracted:
            flag(prev['data_node'], RR_ERR, group)
            count_error(RR_ERR, institution)
        elif not_latest or not original['latest']:
            flag(prev["data_node"], LATEST_ERR, group)
            count_error(LATEST_ERR, institution)
        elif multiples:
            flag(prev["data_node"], "Multiple originals", group)
            count_error("Multiple originals", institution)
        elif nof_err:  # if number of files inconsistent, return as an inconsistency sorted by institution id
            # check timestamp to see if it was us or them
            if (nof_rec['_timestamp'] > original['_timestamp']) and ('llnl' in nof_rec['data_node']):
                # it was us
                flag(institution, NOF_ERR, original)
            else:
                # it was them
                flag(institution, NOF_ERR2, original)
        if failed_ac:
            flag(institution, AC_ERR, group)
        elif failed_ec:
            flag(institution, EC_ERR, group)
        if have_replica and SAVE_REPLICA_HOLDINGS:
            save_to_list(instance)

    print("Done.")


def summarize(error, string, lines, original):
    total_err = len(inconsistencies[error].keys())
    if not original:
        lines.append(str(total_err) + " institutions affected.")
    for i in inconsistencies[error].keys():
        errors = len(inconsistencies[error][i])
        line = i + " had " + str(errors) + string
        lines.append(line)
    return lines


def summarize_alt(error, string, lines):
    for key in error_counts[error].keys():
        line = key + " had " + str(error_counts[error][key]) + string
        lines.append(line)
    return lines


def summary():
    print("Compiling summary...")

    lines = []
    for w in warnings:
        lines.append(w)

    if TEST:
        lines.append("Dry run enabled, no data fetched or processed. No output (0 errors) expected.")
    if DO_AC:
        lines.append("Activity check error logging enabled.")
    else:
        lines.append("Activity check error logging disabled.")
    if ERRATA_CHECK:
        lines.append("Errata database comparison logging enabled.")
    else:
        lines.append("Errata database comparison logging disabled.")
    if FIX_ERRS:
        lines.append("Auto retraction/updating on LLNL ESGF node for errors where no original record was found, "
                     "original records were retracted, or original record was not latest version enabled.")

    lines.append("Duplicate records: ")
    lines = summarize(DUP_ERR, " errors where duplicate records were found.", lines, True)
    lines = summarize_alt(DUP_ERR, " errors where duplicate records were found.", lines)

    lines.append("Records with inconsistent number of files:")
    lines = summarize(NOF_ERR, " errors corresponding to an inconsistent number of files. (esgf replica issue)", lines,
                      False)
    lines = summarize(NOF_ERR2, " errors corresponding to an inconsistent number of files. (client issue)", lines,
                      False)
    if DO_AC:
        lines.append("Records which failed activity check:")
        lines = summarize(AC_ERR, " errors where the record failed the activity check.", lines, False)

        lines.append("Records which failed the experiment_id check:")
        lines = summarize(EC_ERR, " errors where the experiment_id did not agree with the activity_id.", lines, False)

    lines.append("Records with no original record:")
    lines.append(str(len(error_counts[ORIGINAL_ERR].keys())) + " institutions affected.")
    lines = summarize_alt(ORIGINAL_ERR, " errors where no original record was found.", lines)
    lines = summarize(ORIGINAL_ERR, " errors where no original record was found.", lines, True)

    lines.append("Records with original record not latest version:")
    lines.append(str(len(error_counts[LATEST_ERR].keys())) + " institutions affected.")
    lines = summarize_alt(LATEST_ERR, " errors where the original record was not the latest version.", lines)
    lines = summarize(LATEST_ERR, " errors where the original record was not the latest version.", lines, True)

    lines.append("Records with original record retracted:")
    lines.append(str(len(error_counts[RETRACT_ERR].keys())) + " institutions affected.")
    lines = summarize_alt(RETRACT_ERR, " errors where the original record was retracted.", lines)
    lines = summarize(RETRACT_ERR, " errors where the original record was retracted.", lines, True)

    lines.append("Records with replica retracted, original not retracted:")
    lines.append(str(len(error_counts[RR_ERR].keys())) + " institutions affected.")
    lines = summarize_alt(RR_ERR, " errors where one or more replicas were retracted.", lines)
    lines = summarize(RR_ERR, " errors where one or more replicas were retracted.", lines, True)

    lines.append("Records with multiple originals:")
    lines.append(str(len(error_counts["Multiple originals"].keys())) + " institutions affected.")
    lines = summarize_alt("Multiple originals", " errors where multiple originals were found.", lines)
    lines = summarize("Multiple originals", " errors where multiple originals were found.", lines, True)
    
    if ERRATA_CHECK:
        lines.append("Records with existing errata:")
        lines.append(str(len(error_counts[ERRATA].keys())) + " institutions affected.")
        lines = summarize_alt(ERRATA, " errors where an existing errata was found.", lines)
        lines = summarize(ERRATA, " errors where an existing errata was found.", lines, True)

    msg = "Today's inconsistency check results:"
    for line in lines:
        msg += "\n" + line

    print("Done.")
    return msg


def send_data(message, to_email, attachments=None):
    print("Sending email...")

    msg = MIMEMultipart()
    # from_email = "elysiawitham@gmail.com"
    from_email = "witham3@llnl.gov" #ames4@llnl.gov
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = "Data Inconsistency Results -- ESGF"
    body = message
    msg.attach(MIMEText(body, 'plain'))
    if attachments is not None:
        for attachment in attachments:
            attach_file = open(attachment, 'r+')
            payload = MIMEBase('application', 'octate-stream')
            payload.set_payload((attach_file).read())
            encoders.encode_base64(payload)
            payload.add_header('Content-Decomposition', 'attachment', filname=attachment)
            msg.attach(payload)
    # s = smtplib.SMTP('smtp.gmail.com', 587)  # llnl smtp: nospam.llnl.gov
    s = smtplib.SMTP('nospam.llnl.gov')
    s.ehlo()
    s.starttls()
    # s.login(from_email, "")  # fill in passwd before running
    text = msg.as_string()
    s.sendmail(from_email, to_email, text)
    s.quit()

    print("Done.")


def gen_ids(d):
    rm = []
    lf = []
    nodes = ["aims3.llnl.gov", "esgf-data1.llnl.gov"]
    print(d.keys())
    for err in d.keys():
        # if err == ORIGINAL_ERR or err == RETRACT_ERR: # temporarily removed this due to false positives being retracted when nodes are down
        if err == RETRACT_ERR:
            l = rm
        elif err == LATEST_ERR:
            l = lf
        else:
            continue
        for node in d[err].keys():
            if node not in nodes:
                continue
            for recs in d[err][node]:
                rec = recs[0]
                instance = rec["instance_id"]
                id = instance + "|" + node
                l.append(id)
    return rm, lf


def fix_retracted_missing(ids):
    pc = publisherClient(CERT, INDEX_NODE)
    for i in ids:
        pc.retract(i)


def fix_latest_false(ids):
    pc = publisherClient(CERT, INDEX_NODE)
    for i in ids:
        xml = up.gen_hide_xml(i, "datasets")
        print(xml)
        pc.update(xml)
        xml = up.gen_hide_xml(i, "files")
        print(xml)
        pc.update(xml)


if __name__ == '__main__':
    if TEST:
        print("Dry run enabled. No data will be fetched or processed, no output will be given.")
    inconsistencies = {}
    inconsistencies[ORIGINAL_ERR] = {}
    inconsistencies[NOF_ERR] = {}
    inconsistencies[NOF_ERR2] = {}
    inconsistencies[LATEST_ERR] = {}
    inconsistencies[RETRACT_ERR] = {}
    inconsistencies["Multiple originals"] = {}
    inconsistencies[DUP_ERR] = {}
    inconsistencies[RR_ERR] = {}
    inconsistencies[AC_ERR] = {}
    inconsistencies[EC_ERR] = {}
    inconsistencies[ERRATA] = {}
    count = 0
    ntotal = 0

    originals_by_institution = {}
    error_counts = {}
    error_counts[ORIGINAL_ERR] = {}
    error_counts[RETRACT_ERR] = {}
    error_counts[LATEST_ERR] = {}
    error_counts[DUP_ERR] = {}
    error_counts[RR_ERR] = {}
    error_counts[ERRATA] = {}
    error_counts['Multiple originals'] = {}
    warnings = []
    E3SM_f = []
    counts = [0, 0, 0, 0]
    skips = []

    # retracted=false
    # look at IPSL for latest false originals
    search_url = "http://esgf-node.llnl.gov/esg-search/search?project=CMIP6&latest=true&retracted=false&limit={}&offset={}&format=application%2fsolr%2bjson&replica=true&institution_id={}&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,retracted,id,activity_drs,activity_id,source_id,experiment_id"
    
    node_list = get_nodes()
    
    uk_args = "project=CMIP6&limit={}&offset={}&institution_id={}&replica=false&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,version,retracted,id,activity_drs,activity_id,source_id,experiment_id"

    for node in node_list:
        print(node)
        try:
            institution_list = get_list(node)
        except Exception as ex:
            print("Could not gather data from " + node + ": " + str(ex))
            continue
        for institution in institution_list:
            if institution in skips:
                continue
            else:
                base = "http://{}/esg-search/search?".format(node)
                if "ceda" in node:
                    args = uk_args
                else:
                    args = "project=CMIP6&limit={}&offset={}&format=application%2fsolr%2bjson&institution_id={}&replica=false&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,version,retracted,id,activity_drs,activity_id,source_id,experiment_id"
                url = base + args
            print("Fetching originals...")
            originals, tally = get_batch(url, institution)
            if tally == 0:
                continue
            else:
                ntotal += tally
            if institution not in originals_by_institution.keys():
                originals_by_institution[institution] = originals
            else:
                existing = originals_by_institution[institution]
                merged = {}
                for key in (existing.keys() | originals.keys()):
                    if key not in merged.keys():
                        merged[key] = []
                    if key in existing:
                        merged[key] += existing[key]
                    if key in originals:
                        merged[key] += originals[key]
                assert (type(merged) is dict)
                originals_by_institution[institution] = merged
    if len(warnings) > 5:
        print("Too many warnings.")
        print(warnings)
        exit(1)
    institution_list = get_list()
    for institution in institution_list:
        if institution in skips:
            continue
        if institution not in originals_by_institution.keys():
            print("No original records found for: " + institution)
            warnings.append("No original records found for: " + institution)
            continue
        print("Fetching replicas...")
        replicas, tally = get_batch(search_url, institution)
        ntotal += tally
        originals = originals_by_institution[institution]
        total = {}
        for key in (originals.keys() | replicas.keys()):  # merge dicts
            if key not in total.keys():
                total[key] = []
            if key in originals:
                total[key] += originals[key]
            if key in replicas:
                total[key] += replicas[key]
        try:
            find_inconsistencies(total, institution)
        except Exception as ex:
            print("Error fetching inconsistencies for " + institution + ": " + str(ex))
            warnings.append("Error fetching inconsistencies for " + institution + ": " + str(ex))
            continue

    myfile = open(DIRECTORY + 'inconsistencies.json', 'w+')
    lst = []
    ceda = []
    dkrz = []
    nci = []
    llnl = []
    acceptable = [ORIGINAL_ERR, LATEST_ERR, RETRACT_ERR]
    for err in inconsistencies.keys():
        if err not in acceptable:
            continue
        else:
            for node in inconsistencies[err].keys():
                fn_err = err
                fn_err2 = "_".join(fn_err.split(" "))
                fn = DIRECTORY + node + "_" + fn_err2[:-1] + ".json"
                with open(fn, 'w+') as fp:
                    try:
                        json.dump(inconsistencies[err][node], fp, indent=4)
                        if 'ceda' in fn:
                            ceda.append(fn)
                        elif 'dkrz' in fn:
                            dkrz.append(fn)
                        elif 'nci' in fn:
                            nci.append(fn)
                        elif 'llnl' in fn:
                            llnl.append(fn)
                    except Exception as ex:
                        print("Could not write error to file: " + err + node)
    if len(warnings) > 5:
        print("Too many warnings.")
        print(warnings)
        exit(1)
    json.dump(inconsistencies, myfile, indent=4)  # saves data as json file
    zipfile = gzip.open(DIRECTORY + "inconsistencies.json.gz", 'w+')
    shutil.copyfileobj(myfile, zipfile)
    myfile.close()
    zipfile.close()
    if SAVE_REPLICA_HOLDINGS:
        instance_file.close()
    # with open(DIRECTORY + 'E3SM.json', 'w+') as d:
    #    json.dump(E3SM_f, d, indent=4)

    summ = summary()
    if PRIMARY_EMAIL is not None:
        for e in PRIMARY_EMAIL:
            send_data(summ, e)
    # send_data(summ, 'e.witham@columbia.edu')
    # send_data(summ, 'ames4@llnl.gov')

    if len(warnings) > 2:
        pass
    elif EMAIL_LIST:
        send_data(summ, 'alan.iwi@stfc.ac.uk')
        send_data(summ, 'esgf@dkrz.de')
        send_data(summ, 'kelsey.druken@anu.edu.au')

    rm, lf = gen_ids(inconsistencies)
    print("retracted/missing: " + str(rm))
    print("latest false: " + str(lf))
    if FIX_ERRS:
        fix_latest_false(lf)
        fix_retracted_missing(rm)
        print("Latest errors and Retracted & Missing errors fixed.")
    print("TOTAL: " + str(ntotal))
