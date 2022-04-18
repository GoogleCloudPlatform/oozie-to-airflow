headerIndex=$1
dateColumnIndex=$2
headerLinesToSkip=$3
footerLinesToSkip=$4
inputFile=$5
separator=$6
if [[ ${separator} == 'SPECIAL' ]]; then
	separator=''
fi

# Get the date from the header
date=""
if [[ ${headerIndex} -ne -1 ]]; then
	date=`hadoop fs -cat "$inputFile" | head -${headerIndex} | tail -1 | cut -d "${separator}" -f${dateColumnIndex}`
else
	date=`hadoop fs -find "$inputFile" | head -n 1 | cut -f 1 -d '.' | tail -c 9`
fi
echo "date=$date"

# Get the number of lines in the file
numberOfLines=`hadoop fs -cat "$inputFile" | wc -l`
numberOfLines=$(($numberOfLines - $headerLinesToSkip - $footerLinesToSkip))
echo "numberOfLines=$numberOfLines"

# Get the input file's modified date
modifiedDate=`hadoop fs -stat "$inputFile"`
echo "modifiedDate=$modifiedDate"