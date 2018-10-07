#!/bin/bash

wget http://download.geonames.org/export/dump/allCountries.zip
unzip allCountries.zip
rm -f allCountries.zip

exit 0