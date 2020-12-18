import requests, json, sys
from esgcet.pub_client import publisherClient
import esgcet.update as up
import esgcet.activity_check as ac
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import gzip
import shutil


if len(sys.argv) < 3:
    print("Missing args! \nusage: python3 cic.py </write/directory/> </path/to/cmor/tables/> \nNOTE: use absolute paths and be sure to include the ending '/'.")
    exit(1)

TEST = False
EMAIL = False
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
duplicates = []
INDEX_NODE = "esgf-node.llnl.gov"
CERT = "/p/user_pub/publish-queue/certs/certificate-file"
CMOR_PATH = sys.argv[2]
DIRECTORY = sys.argv[1]
instance_file = open(DIRECTORY + "need_replicas.txt", "w")

def save_to_list(instance):
    instance_file.write(instance + "\n")


def run_ac(input_rec):
    cv_path = "{}CMIP6_CV.json".format(CMOR_PATH)
    jobj = json.load(open(cv_path))["CV"]
    sid_dict = jobj["source_id"]

    src_id = input_rec['source_id'][0]
    act_id = input_rec['activity_drs'][0]

    if src_id not in sid_dict:
        return False

    rec = sid_dict[src_id]
    return act_id in rec["activity_participation"]


def run_ec(rec):
    cv_path = "{}CMIP6_CV.json".format(CMOR_PATH)

    act_id = rec['activity_drs'][0]
    exp_id = rec['experiment_id'][0]

    cv_table = json.load(open(cv_path, 'r'))["CV"]
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

    if skipped > 2:
        print("ERROR: more than 2 data nodes unreachable. Exiting.")
        exit(1)
    else:
        warnings.append("WARNING: following data nodes unreachable: " + str(bad_lst) + ". This may impact results.")
        return lst


def get_batch(search_url, institution):
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


def find_inconsistencies(batch, institution):
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
        replica_needed = True
        for member in group:
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
                replica_needed = False
            if not run_ac(member):
                failed_ac = True
            elif not run_ec(member):
                failed_ec = True
            if not compare(member, prev, "number_of_files"):  # consistency check for number of files field
                nof_rec = member
                nof_err = True
            prev = member
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
        if replica_needed:
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

    lines.append("Duplicate records: ")
    lines = summarize(DUP_ERR, " errors where duplicate records were found.", lines, True)
    lines = summarize_alt(DUP_ERR, " errors where duplicate records were found.", lines)

    lines.append("Records with inconsistent number of files:")
    lines = summarize(NOF_ERR, " errors corresponding to an inconsistent number of files. (esgf replica issue)", lines,
                      False)
    lines = summarize(NOF_ERR2, " errors corresponding to an inconsistent number of files. (client issue)", lines,
                      False)
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

    msg = "Today's inconsistency check results:"
    for line in lines:
        msg += "\n" + line

    print("Done.")
    return msg


def send_data(message, to_email, server, attachments=None):
    print("Sending email...")

    msg = MIMEMultipart()
    # from_email = "elysiawitham@gmail.com"
    from_email = "witham3@llnl.gov" #ames4@llnl.gov
    msg['From'] = from_email
    msg['To'] = to_email
    if server != 'gmail':
        print("Server not implemented, email failed.")
    else:
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
    for error in d.keys():
        if error == ORIGINAL_ERR or error == RETRACT_ERR:
            l = rm
        elif error == LATEST_ERR:
            l = lf
        else:
            continue
        for node in inconsistencies[err].keys():
            if node not in nodes:
                continue
            for recs in inconsistencies[err][node]:
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
        xml = up.gen_hide_xml(i)
        pc.update(xml)


if __name__ == '__main__':
    inconsistencies = {}
    inconsistencies[ORIGINAL_ERR] = {}
    inconsistencies[NOF_ERR] = {}
    inconsistencies[LATEST_ERR] = {}
    inconsistencies[RETRACT_ERR] = {}
    inconsistencies["Multiple originals"] = {}
    inconsistencies[DUP_ERR] = {}
    inconsistencies[RR_ERR] = {}
    inconsistencies[AC_ERR] = {}
    inconsistencies[EC_ERR] = {}
    count = 0
    ntotal = 0

    originals_by_institution = {}
    error_counts = {}
    error_counts[ORIGINAL_ERR] = {}
    error_counts[RETRACT_ERR] = {}
    error_counts[LATEST_ERR] = {}
    error_counts[DUP_ERR] = {}
    error_counts[RR_ERR] = {}
    error_counts['Multiple originals'] = {}
    warnings = []
    E3SM_f = []
    counts = [0, 0, 0, 0]
    skips = []

    # retracted=false
    # look at IPSL for latest false originals
    search_url = "http://esgf-node.llnl.gov/esg-search/search?project=CMIP6&latest=true&retracted=false&limit={}&offset={}&format=application%2fsolr%2bjson&replica=true&institution_id={}&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,retracted,id,activity_drs,activity_id,source_id,experiment_id"

    uk_url = "https://esgf-node.llnl.gov/esg-search/search/?limit={}&offset={}&replica=false&latest=true&data_node=esgf-data3.ceda.ac.uk&project=CMIP6&format=application%2fsolr%2bjson&institution_id={}&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,version,retracted,id,activity_drs,activity_id,source_id,experiment_id"
    node_list = get_nodes()

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
                args = "project=CMIP6&limit={}&offset={}&format=application%2fsolr%2bjson&institution_id={}&replica=false&fields=instance_id,number_of_files,_timestamp,data_node,replica,institution_id,latest,version,retracted,id,activity_drs,activity_id,source_id,experiment_id"
                url = base + args
                if "ceda" in node:
                    url = uk_url
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
                fn = DIRECTORY + node + "-" + err + ".json"
                with open(fn, 'w+') as fp:
                    try:
                        fp.write(fn)
                        fp.write(err)
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
    instance_file.close()
    with open(DIRECTORY + 'E3SM.json', 'w+') as d:
        json.dump(E3SM_f, d, indent=4)

    summ = summary()
    try:
        send_data(summ, 'e.witham@columbia.edu', 'gmail', llnl)
        send_data(summ, 'amysash2006@gmail.com', 'gmail')
    except Exception as ex:
        send_data(summ, 'e.witham@columbia.edu', 'gmail')
        send_data(summ, 'amysash2006@gmail.com', 'gmail')


    if len(warnings) > 2:
        pass
    elif EMAIL:
        send_data(summ, 'ruth.petrie@stfc.ac.uk', 'gmail')
        send_data(summ, 'esgf@dkrz.de', 'gmail')
        send_data(summ, 'kelsey.druken@anu.edu.au', 'gmail')

    rm, lf = gen_ids(inconsistencies)
    fix_latest_false(lf)
    fix_retracted_missing(rm)
    print("Latest errors and Retracted & Missing errors fixed.")
    print("TOTAL: " + str(ntotal))
