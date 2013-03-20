wibidota
========

Tools for working with Valve's Dota 2 data set for the WibiData team. Presently consists only of `dota_slurp.py`, a tool for efficiently pulling in a lot of data.

Requirements
============

* Python's `requests` library (pip install requests)
* Recommended: The `ujson` library (pip install ujson)

You'll also need a steam api key. Get one from [http://steamcommunity.com/dev/apikey](http://steamcommunity.com/dev/apikey) and store it in the environmental variable `DOTA2_API_KEY` (I added mine to `~/.bash_profile`).

`dota_slurp.py` Usage
=====================

1. Install requirements and API key as described above
2. Coordinate with team to decide who is grabbing which range and set up `config.json` accordingly.
3. Run `python dota_slurp.py` and go grab a sandwich.

The script will create files named things like `matches_0-1000000`, `matches_1000000-2000000`, etc. in the current directory.

config.json
===========

This file is both read and written by `dota_slurp.py` and used to maintain state betwee runs. It's a json file containing three values:

* "start" is the next sequence number to retrieve. This is updated every time a complete block of matches is completed.
* "block\_size" determines the limit in the sequence id space per file. A file will contain all matches that fall in the range [start, start+block\_size).
* "skip\_blocks" How many blocks to skip between start points. If you wanted to grab contiguous blocks, this should be 0. If you wanted to grab every other block, set to 1, etc.

Hence the example `config.json`:

`{"start":0,"block_size":1000000,"skip_blocks":1}`

will grab a block of matches from 0 to 1000000 (exclusive) then skip a block and begin grabbing a block of matches from 2000000 to 3000000, covering half the match history space.

Known Issues and TODOs
======================

* More info while running would be nice. Currently only prints data every completed file and if those are set large, it could take a while.
* Revisit config.json design.
* Compression, compression, compression. I'm seeing a factor of savings of over 10x using bz2 and a little under 8x using gzip. Files are pretty large (about a gigabyte for a million matches), so this is probably a good idea.
* It's unclear what to do to continue work later when we reach the end of match history. But this will probably need a different tool.
* Obviously, and unfortunately, we don't have skill tags for the matches.
