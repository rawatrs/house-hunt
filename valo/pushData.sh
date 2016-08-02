#!/bin/bash
declare -A mapQueries

#######################################################################
#
# This script pushes property data to Valo (www.valo.io)
# Run this script from top-level as: ./valo/pushData.sh "true" data/zoopla
#
# Input argument1 : Whether to create Valo schema (true/false)
# Input argument2 : Directory where properties data is stored
#
# Assumptions: Valo is already running
#
#######################################################################


VALOHOST=localhost
VALOPORT=8888
VALOTENANT=demo
SESSION_ID=""
COLLECTION=homes
STREAM_NAME=zoopla
SCHEMA_FILE=zooplaDataSchema.json
REPOSITORY_MAPPING_FILE=valo/repositoryMappingFile.json

function VALO_checkCollectionExists {
	#$1- COLLECTION

	echo ">>>>>>>>>> Checking Collection exists"
	echo "curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT"
	echo ""
	COLLECTIONS=`curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT`
	# TODO: Check collections has the collection passed in
	echo "COLLECTIONS : " $COLLECTIONS
	echo ""
	
	EXISTS=`echo '$COLLECTIONS' | jq ".instances.$1"`
	if [ "$EXISTS" == "null" ] ; then echo "ERROR: Collection does not exist"; exit; fi
}

function VALO_checkStreamExists {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	
	echo ">>>>>>>>>> Checking Stream exists"
	echo "curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1"
	echo ""
	STREAMS=`curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1`
	echo "STREAMS : " $STREAMS
	echo ""
	
	EXISTS=`echo '$STREAMS' | jq ".instances.$2"`
	if [ "$EXISTS" == "null" ] ; then echo "ERROR: Stream does not exist"; exit; fi
}

function VALO_verifySchema {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	#$3 - SCHEMA_FILE
	
	echo ">>>>>>>>>> Verifying Schema"
	echo "curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2"
	echo ""
	ACTUAL_SCHEMA=`curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2`
	EXPECTED_SCHEMA=`cat $3 | jq -c .`

	if [ "$ACTUAL_SCHEMA" != "$EXPECTED_SCHEMA" ]; then echo "WARNING: Schema does not match"; fi
}

function VALO_createSchema {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	#$3 - SCHEMA_FILE
	
	SCHEMA_URI="http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2"
	echo ">>>>>>>>>> Creating Schema"
	echo "curl -s -H "Content-Type: application/json" -X PUT --data @$3 $SCHEMA_URI"
	echo ""
	SESSION=`curl -s -H "Content-Type: application/json" -X PUT --data @$3 $SCHEMA_URI`
	
	VALO_checkCollectionExists $1
	VALO_checkStreamExists $1 $2
	VALO_verifySchema $1 $2 $3
}

function verifyRepositoryMapping {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	#$3 - REPOSITORY_MAPPING_FILE
	
	echo ">>>>>>>>>> Verifying Repository Mapping"
	echo "curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2/repository"
	echo ""
	actualRepoMapping=`curl -s http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2/repository`

	echo "cat $3 | jq -c ."	
	expectedRepoMapping=`cat $3 | jq -c .`
	if [ "$actualRepoMapping" != "$expectedRepoMapping" ]; then echo "WARNING: Repository Mapping does not match"; fi
}

function VALO_mapRepository {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	#$3 - REPOSITORY_MAPPING_FILE
	
	MAP_REPO_URI="http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2/repository"
	
	echo ">>>>>>>>>> Repository Mapping"
	echo "curl -s -X PUT -H "Content-Type: application/json" --data @$3 $MAP_REPO_URI"
	echo ""
	CMD=`curl -s -X PUT -H "Content-Type: application/json" --data @$3 $MAP_REPO_URI`
	
	verifyRepositoryMapping $1 $2 $3
}

function VALO_pushData {
	#$1 - COLLECTION
	#$2 - STREAM_NAME
	#$3 - PROPERTY_FILE
	
	PUSH_DATA_URI="http://$VALOHOST:$VALOPORT/streams/$VALOTENANT/$1/$2"
	#echo ">>>>>>>>>> Pushing Data to Valo"
	#echo "curl -s -X POST -H "Content-Type: application/json" --data @$3 $PUSH_DATA_URI"
	# echo ""
	CMD=`curl -s -X POST -H "Content-Type: application/json" --data @$3 $PUSH_DATA_URI`
}

function pushData {
	#$1 - DIR
	#$2 - COLLECTION
	#$3 - STREAM_NAME

	totalFiles=`ls ${1}/*.json | wc -l`
	echo "Total files to upload: $totalFiles"	

	for filepath in $1/*.json; do
		filename=$(basename "$filepath")
		extension="${filename##*.}"
		filename="${filename%.*}"

                #Replace datetime with the accepted valo format
                sed -i -r "s/([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2})/\1T\2Z/g" $filepath
                #Insert dynamic empty records with dummy values
                sed -i -r "s/\{\}/\"NO_INFO\"/g" $filepath

		newfile=$1/${filename}_R.${extension}

                #Delete array records because Valo doesn't support them yet!
                cat $filepath | jq 'del(.price_change)' | jq 'del(.floor_plan)' | jq 'del(.price_change_summary)' > $newfile

                echo $newfile

		VALO_pushData $2 $3 $newfile
	done
}

#$1 - Whether to create schema
if [[ $1 == true ]]; then
	VALO_createSchema $COLLECTION $STREAM_NAME $SCHEMA_FILE
	VALO_mapRepository $COLLECTION $STREAM_NAME $REPOSITORY_MAPPING_FILE
fi

echo "Pushing $2 properties data to Valo....."
pushData $2 $COLLECTION $STREAM_NAME
echo "Done"

