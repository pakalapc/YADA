#******************************************#
# Script: checkinginfo_populate_script.sh  #
# Description: To parse the jason file     #
#              and get the data needed for #
#              yada_checkinginfo table     #
# Author: Chandni Pakalapati               #   			                             
#******************************************#

for line in `cat yelp_academic_dataset_checkin_space.json`
do
        #echo $line
        count=`echo $line | cut -d "{" -f3 | cut -d "}" -f1 | grep -o "," | wc -l`
        #echo $count
        c=`expr $count + 1`
        business_id=`echo $line| sed 's/business_id/#/g' | cut -d "#" -f2 | cut -d ":" -f2 | sed 's/}//' | sed 's/ //'`
        #echo $c
        index=1;
        #echo $business_id
        while [ $index -le $c ]
        do
                hour=`echo $line | cut -d "{" -f3| cut -d "}" -f1 | cut -d "," -f$index | cut -d "\"" -f2 | cut -d "-" -f1`
                day=`echo $line | cut -d "{" -f3| cut -d "}" -f1 | cut -d "," -f$index | cut -d "\"" -f2 | cut -d "-" -f2`
                value=`expr $hour \* 24 + $day`

                checkin=`echo $line | cut -d "{" -f3| cut -d "}" -f1 | cut -d "," -f$index | cut -d ":" -f2 | sed 's/ //'`

                data="$business_id,$checkin,$value"
                echo $data >>checking_data
                index=`expr $index + 1`
        done
done
