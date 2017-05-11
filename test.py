

import datetime, time, pytz

utc = pytz.timezone('UTC')
now = utc.localize(datetime.datetime.now())
mi = now.minute
print type(mi)


while True:

    time.sleep(3)

    print "Time now is: " + str(datetime.datetime.now())
    print "Minutes: " + str(datetime.datetime.now().minute)
    if datetime.datetime.now().minute == 33:

        print "Minutes are 33: hoorah"
    else:
        continue



