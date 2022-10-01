#!/bin/bash

########### python script to merge climate csv data into all_year.csv file ##############
PYTHON_SCRIPT='run.py'

########### path where script resides, csv data and all_year.csv file will be created #############
SCRIPT_PATH=$(cd $(dirname "${BASH_SOURCE:-$0}") && pwd)

########### path to store input data extracted by year_wise_data function ############
INPUT_FOLDER=$(cd "$( dirname "${BASH_SOURCE:-$0}" )" && cd ../INPUT_FOLDER && pwd)


########### path to store output data extracted by year_wise_data function ############
OUTPUT_FOLDER=$(cd "$( dirname "${BASH_SOURCE:-$0}" )" && cd ../OUTPUT_FOLDER && pwd)



###################### year_wise_help(): function will help to understand how to run script ###############
year_wise_help(){
	clear
	echo "year_wise_data.sh"
	echo
	echo "Shell script $0: You will use the shell script to control every operation, including data downloading, log setting, python script running."
	echo
	echo "Python script ${python_script}: While the Python script is used to call API and process data."
	echo
	echo "Options:"
	echo 
	echo " -p       start the script to process data"
	echo " -h       Help -- this screen"
}


################ merge_year_wise_data: function will call python script to merge year wise data created by year_wise_data function ################
run(){
  source sandbox/bin/activate
	echo "Executing python script $python_script ...."
	python3 "${PYTHON_SCRIPT}"
	echo "Finished processing data!!!"

	RC=$?
	if [ ${RC} -ne 0 ]; then
		echo "[ERROR]: Scripted failed"
		echo "[ERROR]: Return code ${RC}"
		exit 1
	fi

  deactivate
}


############## main(): function will initialize the script #############
main(){
	while getopts hp flags
	do
   		case "${flags}" in
        	h) year_wise_help
           		exit 0;;
        	p) run;;
        	*) year_wise_help
           		exit 0;;
   		esac
	done
}

# program starts here
main $*

exit 0