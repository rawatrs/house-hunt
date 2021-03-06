#!/bin/bash
source ./settings.sh

#######################################################################
#
# This script scrapes Zoopla properties and stores them to disk 
# Run this script a: ./scrape_zoopla.sh <API_KEY> LondonPostCodes.txt
#
#######################################################################

API_KEY=$1
FILE=$2

function ZOOPLA_propertyListing {
	#$1 - POSTCODE
	#$2 - PAGESIZE
	#$3 - PAGENUMBER
	
	REQUEST_URI="http://api.zoopla.co.uk/api/v1/property_listings.xml?postcode=$1&page_size=$2&page_number=$3"

	for k in ${!FILTER[@]}; do
		REQUEST_URI="$REQUEST_URI&${k}=${FILTER[${k}]}"
	done

	REQUEST_URI="$REQUEST_URI&api_key=$API_KEY"

	#echo $REQUEST_URI

	RESPONSE=`curl -s -X GET $REQUEST_URI | ~/node_modules/.bin/xml2json | jq ".response" `
	echo $RESPONSE
}

mkdir -p data/zoopla

echo "Querying Zoopla....."
echo ""

OLD_IFS=$IFS
IFS=$'\n'
londonPostCodesArr=( $(cat ${FILE}) )
IFS=$OLD_IFS

noCalls=0
for postCode in "${londonPostCodesArr[@]}"
do
	echo "Checking properties in $postCode.."
	echo ""
		
	iteration=1
	nListingsToFetch=100
	
	while [ $nListingsToFetch -gt 0 ]
	do
		pageSize=$nListingsToFetch
		pageNumber=$iteration
		
		propertyRecords=$( ZOOPLA_propertyListing $postCode $pageSize $pageNumber)
		noCalls=$((noCalls +1))	
		
		totalListings=$nListingsToFetch
		if [ $iteration -eq 1 ]; then
			totalListings=`echo $propertyRecords | jq -r ".result_count"`
			echo "Total number of properties in $postCode = $totalListings"
		fi

		propertyListings=`echo $propertyRecords | jq ".listing"`
		echo ""

		nPropertyListings=`echo $propertyListings | jq '. | length'`
		echo "Number of Properties in $postCode in iteration $iteration = $nPropertyListings"

		nListingsToFetch=`expr $totalListings - $nPropertyListings`
	
		index=0
		while [ $index -lt $nPropertyListings ] 
		do
			property=`echo $propertyListings | jq --arg index $index '.[$index | tonumber]'`
			echo $property > data/zoopla/property_${postCode}_${iteration}_${index}.json

			index=$((index +1))
		done
		
		echo "Number of properties still left to fetch = $nListingsToFetch"

		sleep 1

		iteration=$((iteration+1))

		if (( noCalls % 1000 == 0 ))
		then
			sleep 1h #Zoopla API restriction
		fi

	done
done


