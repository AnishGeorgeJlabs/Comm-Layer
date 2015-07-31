#!/bin/bash
screen -X -S wadi_sms quit > /dev/null
sudo rm -r ./supervise > /dev/null
sudo ./resetRabbit.sh
screen -d -m -S wadi_sms
screen -S wadi_sms -X zombie qr

screen -S wadi_sms -X screen ./logger.sh
screen -S wadi_sms -X screen supervise .

for i in $(eval echo {1..$1}); do
  screen -S wadi_sms -X screen ./sender.sh
done

for i in $(eval echo {1..$2}); do
  screen -S wadi_sms -X screen ./handler.sh
done
