"""
WibiDota - dota_slurp. Tool for slurping in a lot of dota matches.
Requires environmental variable 'DOTA2_API_KEY' to be set.
"""

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json
import gzip
import logging
import os
import requests
import sys
import time

API_KEY = os.environ.get("DOTA2_API_KEY")
BASE_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v0001/"

# Time to wait between requests to the dota 2 web API. Number of seconds.
REQUEST_PERIOD = 1.0
# Global that tracks the next time we can make a request.
NEXT_REQUEST_TIME = 0

# The config file we use to store/restore our place between runs and the
# globals for its settings.
CONFIG_FILE = "config.json"

# These globals are for calculating aggregate performance numbers for the time
# we spend waiting between requests.
TOTAL_WAIT_TIME = 0.0
TOTAL_CALLS = 0

def read_config():
    """
    Reads in the starting point, block size, and step value from a json config file.
    """
    global NEXT_SEQ, BLOCK_SIZE, NUM_SKIP_BLOCKS
    try:
        conf_file = open(CONFIG_FILE, 'r')
        conf = json.load(conf_file)
        conf_file.close()
        return conf["ranges"]
    except (IOError, ValueError):
        print("""Missing or corrupt %s file. Please use the format:\n"""
        """{\n"""
        """    "ranges":<ranges to retrieve>\n"""
        """}""" % CONFIG_FILE)
        sys.exit(1)

def write_config(ranges):
    """
    Writes out a config file to save our place between runs.
    """
    conf = {"ranges": ranges}
    conf_file = open(CONFIG_FILE, 'w')
    json.dump(conf, conf_file)
    conf_file.close()

def request_matches(start_id):
    """
    Issues a single request against the Dota API and returns the result as a
    json object. Also responsible for updating the timing information.
    """
    global NEXT_REQUEST_TIME, TOTAL_WAIT_TIME, TOTAL_CALLS
    params = dict(key=API_KEY, start_at_match_seq_num=start_id)
    time_to_wait = NEXT_REQUEST_TIME - time.time()
    if time_to_wait > 0:
        # Throttle the requests if necessary, and keep track of wait time.
        time.sleep(time_to_wait)
        TOTAL_WAIT_TIME += time_to_wait
    NEXT_REQUEST_TIME = time.time() + REQUEST_PERIOD
    resp = requests.get(BASE_URL, params=params)
    if resp.status_code != requests.codes.ok:
        print "Warning: Bad status code for request: %s" % resp.status_code
    TOTAL_CALLS += 1
    return json.loads(resp.content)
    
def slurp_block(start_seq_id, limit_seq_id, file):
    """
    Begins retrieving matches and saves them to an open file. The caller is
    responsible for supplying an open file parameter and closing it.
    Matches will be written to the file one per line.
    start_seq_id: The first seq id number to retrieve. Inclusive.
    limit_seq_id: The seq id number to stop at. Exclusive.
    file: The file to write json match representations to.
    Returns true if there are more matches. Returns false if we get 0 matches,
    which should indicate the end of history.
    """
    next_start = start_seq_id
    while True:
        matches = request_matches(next_start)['result']['matches']
        if not matches:
            return False
        for m in matches:
            if m['match_seq_num'] >= limit_seq_id:
                return True
            json.dump(m, file)
            file.write("\n")
            next_start = m['match_seq_num'] + 1

if __name__ == "__main__":
    # Read in a config file to remember our place, and begin recording.
    ranges = read_config()
    print "Retrieving Dota 2 match history...."
    while True:
        try:
            range = ranges.pop(0)
        except IndexError:
            print "Finished with all ranges in config.json."
            print "Exiting."
            sys.exit(0)
        print "Downloading range [%d,%d)." % (range[0], range[1])
        f = gzip.open("matches_%d-%d.gz"
                      % (range[0], range[1]), 'w')
        if not slurp_block(range[0], range[1], f):
            print "No matches found beyond seq %d" % range[1]
            print "Exiting."
            f.close()
            sys.exit(0)
        f.close()
        write_config(ranges)
        # Report some stats.
        print("Total API requests: %s\n"
              "Total wait time: %s\n"
              "Average wait per request: %s"
              % (TOTAL_CALLS, TOTAL_WAIT_TIME, TOTAL_WAIT_TIME/TOTAL_CALLS))

