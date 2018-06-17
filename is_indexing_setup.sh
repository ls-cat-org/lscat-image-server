#! /usr/bin/bash

export PATH=/usr/bin:/usr/local/bin

source /pf/opt/DIALS/dials_env.sh
source /pf/opt/PHENIX/phenix_env.sh
source /pf/opt/CCP4/setup-scripts/ccp4.setup-sh
source /pf/opt/ARPWARP/arpwarp_setup.bash
export besthome=/pf/opt/BEST
export PATH=${PATH}:besthome
source /usr/local/src/RAPD/rapd.shrc


