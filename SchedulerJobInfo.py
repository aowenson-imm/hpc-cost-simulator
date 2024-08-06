'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Generic class to capture required information for scheduler jobs.

Different schedulers report time in different formats so this class
standardized on the Slurm formats instead of the epoch seconds used by
LSF because it is much easier to read.

There are 2 types of date/time formats used.

* Date and Time: YYYY-MM-DDTHH:MM::SS
* Duration: [DD-[HH:]]MM:SS

.. _Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html
'''
__docformat__ = 'google'

from datetime import datetime, timedelta, timezone
import logging
import re

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
#logger.propagate = False
logger.setLevel(logging.INFO)

class SchedulerJobInfo:
    '''
    Class used by the scheduler to store job information

    The field names are based on LSF names because that was the first parser we implemented.
    This data structure puts the data into a common format that isn't parser dependent so that analysis
    isn't parser dependent.

    Note that not all schedulers may provide all of the fields but they must at least provide the
    required fields.
    Scripts that use this should validate the existence of optional fields.
    '''
    def __init__(self,
        # Required fields
        JobID:int,
        NCPUS:int,
        ReqMemGB:float,
        AllocNodes:int,
        Submit:str,
        Start:str,
        End:str,

        # Optional fields
        # ineligible_pend_time:str=None,
        Eligible:str=None,
        # requeue_time:str=None,
        # wait_time:str=None,
        Elapsed:str=None,
        Timelimit:str=None,

        State:str=None,
        Reason:str=None,
        User:str=None,
        Partition:str=None,
        NodeList:str=None,
        Account:str=None,
        # licenses:str=None,

        # exit_status:int=None,
        ExitCode:str=None,

        MaxDiskRead:int=None,
        MaxPages:int=None,
        # ru_maxrss:int=None,
        MaxRSS:int=None,
        # ru_msgrcv:int=None,
        # ru_msgsnd:int=None,
        # ru_nswap:int=None,
        MaxDiskWrite:int=None,
        SystemCPU:float=None,
        UserCPU:float=None,
        TotalCPU:float=None,

        # Put Constraints at end because can contain ',' which is also the CSV separator
        Constraints:str='',
        ):

        # Required fields
        self.JobID = JobID
        self.NCPUS = NCPUS
        self.ReqMemGB = ReqMemGB
        self.AllocNodes = AllocNodes
        (self.Submit, self.Submit_dt) = SchedulerJobInfo.fix_datetime(Submit)
        if Start is None:
            print(SchedulerJobInfo.fix_datetime(Start))
            raise Exception('review')
        (self.Start, self.Start_dt) = SchedulerJobInfo.fix_datetime(Start)
        (self.End, self.End_dt) = SchedulerJobInfo.fix_datetime(End)
        (self.Timelimit, self.Timelimit_td) = SchedulerJobInfo.fix_duration(Timelimit)

        # Optional fields
        # try:
        #     (self.ineligible_pend_time, self.ineligible_pend_time_td) = SchedulerJobInfo.fix_duration(ineligible_pend_time)
        # except Exception:
        #     logger.warning(f"Invalid ineligible_pend_time: {ineligible_pend_time}")
        #     self.ineligible_pend_time = self.ineligible_pend_time_td = None
        try:
            (self.Eligible, self.Eligible_dt) = SchedulerJobInfo.fix_datetime(Eligible)
        except Exception:
            logger.warning(f"Invalid eligible: {Eligible}")
            self.Eligible = self.Eligible_dt = None
        # try:
        #     (self.requeue_time, self.requeue_time_td) = SchedulerJobInfo.fix_duration(requeue_time)
        # except Exception:
        #     logger.warning(f"Invalid requeue_time: {requeue_time}")
        #     self.requeue_time = self.requeue_time_td = None
        # try:
        #     (self.wait_time, self.wait_time_td) = SchedulerJobInfo.fix_duration(wait_time)
        # except Exception:
        #     logger.warning(f"Invalid wait_time: {wait_time}")
        #     self.wait_time = self.wait_time_td = None
        try:
            (self.Elapsed, self.Elapsed_td) = SchedulerJobInfo.fix_duration(Elapsed)
        except Exception:
            logger.warning(f"Invalid elapsed: {Elapsed}")
            self.Elapsed = self.Elapsed_td = None

        self.State = State
        self.Reason = Reason
        self.User = User
        self.Partition = Partition
        self.NodeList = NodeList
        self.Account = Account
        # self.licenses = licenses

        self.ExitCode = ExitCode

        self.MaxDiskRead = MaxDiskRead
        self.MaxPages = MaxPages
        # self.ru_maxrss = ru_maxrss
        self.MaxRSS = MaxRSS
        # self.ru_msgrcv = ru_msgrcv
        # self.ru_msgsnd = ru_msgsnd
        # self.ru_nswap = ru_nswap
        self.MaxDiskWrite = MaxDiskWrite
        (self.SystemCPU, self.SystemCPU_td) = SchedulerJobInfo.fix_duration(SystemCPU)
        (self.UserCPU, self.UserCPU_td) = SchedulerJobInfo.fix_duration(UserCPU)
        (self.TotalCPU, self.TotalCPU_td) = SchedulerJobInfo.fix_duration(TotalCPU)

        self.Constraints = Constraints

        # if not self.ineligible_pend_time:
        #     if self.Eligible:
        #         self.ineligible_pend_time_td = self.Eligible_dt - self.Submit_dt
        #         self.ineligible_pend_time = timedelta_to_string(self.ineligible_pend_time_td)
        #     else:
        #         (self.ineligible_pend_time, self.ineligible_pend_time_td) = SchedulerJobInfo.fix_duration("00:00")
        if not self.Eligible:
            # if self.ineligible_pend_time:
            #     self.Eligible_dt = self.Submit_dt + self.ineligible_pend_time_td
            #     self.Eligible = datetime_to_str(self.Eligible_dt)
            # else:
            self.Eligible = self.Submit
            self.Eligible_dt = self.Submit_dt

        # Bug 31 saved the start even if it was 0 and less than Submit time.
        # Check for this condition and set the start time to the eligible time
        if self.Start_dt < self.Submit_dt:
            self.Start = self.Eligible
            self.Start_dt = self.Eligible_dt

        # # Bug 22 incorrectly calculated the wait_time using start instead of Submit so just always calculate it so it's correct.
        # self.wait_time_td = self.start_dt - self.Eligible_dt
        # self.wait_time = timedelta_to_string(self.wait_time_td)

        if (not self.Elapsed) and self.End_dt:
            self.Elapsed_td = self.End_dt - self.Start_dt
            self.Elapsed = timedelta_to_string(self.Elapsed_td)

        if self.MaxRSS is None:
            # Can make an educated guess
            if self.State == 'OUT_OF_MEMORY':
                self.MaxRSS = self.ReqMemGB / self.AllocNodes
            elif self.Elapsed_td < timedelta(seconds=60):
                self.MaxRSS = 0

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        return d

    @staticmethod
    def fix_datetime(value):
        '''
        Check and fix a DateTime passed as an integer or string.

        DateTime should be stored in the ISO format: `YYYY-MM-DDTHH:MM::SS`

        This is used by the constructor.

        LSF passes times in as an integer integer timestamp.
        If the value is -1 then return None.
        If the integer value is passed as a string then it will be converted to an integer.
        It is checked by converting it to a datetime.datetime object.

        Slurm passes times in ISO format.
        If the string is blank then return None.
        The value is checked by calling `str_to_datetime`.

        The datetime object is then converted back to a string using `timedelta_to_str` to ensure that is formatted correctly with padded integers.

        The value is checked by trying to create a datetime.datetime object using `str_to_timedelta`.

        Args:
            value (int|str): An integer timestamp or string representing a duration.

        Raises:
            ValueError: If value is not a supported type or value.

        Returns:
            tuple(str, datetime): typle with ISO format DateTime string: `YYYY-MM-DDTHH:MM::SS` and datetime object
        '''
        if value is None or value == 'Unknown':
            return (None, None)
        dt_str = None
        dt = None
        if isinstance(value, int):
            # LSF provides a value of -1 to mean None. Otherwise seconds since the epoch.
            if value == -1:
                return (None, None)
            dt = timestamp_to_datetime(value)
        elif isinstance(value, str):
            if re.match(r'^\s*$', value) or value == '-1':
                return (None, None)
            # Check if integer passed with wrong type
            try:
                value = int(value)
                return SchedulerJobInfo.fix_datetime(value)
            except ValueError:
                pass
            # SLURM: Make sure it's the right format
            dt = str_to_datetime(value)
        else:
            raise ValueError(f"Invalid type for datetime: {value} has type '{type(value)}', expected int or str")
        dt_str = datetime_to_str(dt)
        return (dt_str, dt)

    @staticmethod
    def fix_duration(duration):
        '''
        Check and fix a duration passed as an integer or string.

        Durations should be of the following format: `[DD-[HH:]]MM:SS`
        Chose to use this value instead of an integer for readability.

        This is used by the constructor.

        LSF passes durations in as an integer. If the duration is -1 then return None.
        If the integer duration is passed as a string then it will be converted to an integer.
        It is checked by converting it to a datetime.timedelta object.

        Slurm passes in a duration as a string formatted as above.
        The duration is checked by calling `str_to_timedelta`.

        The timedelta object is then converted back to a string using `timedelta_to_str` to ensure that is formatted correctly with padded integers.

        The duration is checked by trying to create a datetime.timedelta object using `str_to_timedelta`.

        Args:
            duration (int|str): An integer timestamp or string representing a duration.

        Raises:
            ValueError: If duration is not a supported type or value.

        Returns:
            tuple(str, timedelta): tuple with time formatted as `[DD-[HH:]]MM:SS` and corresponding timedelta object
        '''
        if duration is None:
            return (None, None)
        if isinstance(duration, (int, float)):
            if duration == -1:
                return (None, None)
            seconds = float(duration)
            td = timedelta(seconds=seconds)
        elif isinstance(duration, str):
            # Check if integer or float passed as a string
            try:
                duration_int = int(duration)
                return SchedulerJobInfo.fix_duration(duration_int)
            except ValueError:
                pass
            try:
                duration_float = float(duration)
                return SchedulerJobInfo.fix_duration(duration_float)
            except ValueError:
                pass
            if duration in ['', 'None']:
                return (None, None)
            # Check format
            td = str_to_timedelta(duration)
        else:
            raise ValueError(f"Invalid type for duration: {duration} has type '{type(duration)}', expected int, float, or str")
        if td is None:
            duration_str = ''
        else:
            duration_str = timedelta_to_string(td)
        return (duration_str, td)

def timestamp_to_datetime(timestamp) -> datetime:
    if timestamp is None:
        return timestamp
    if not isinstance(timestamp, (int, float)):
        raise ValueError(f"Invalid type for timestamp: {timestamp} has type '{type(timestamp)}', expected int or float")
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def str_to_datetime(string_value: str) -> datetime:
    if not isinstance(string_value, str):
        raise ValueError(f"Invalid type for string_value: {string_value} has type '{type(string_value)}', expected str")
    if string_value in ['Unknown', 'None']:
        return None

    date_pattern = r'^\d{4}-\d{2}-\d{2}$'
    match = re.search(date_pattern, string_value)
    if match:
        string_value += "T00:00:00"

    return datetime.strptime(string_value, SchedulerJobInfo.DATETIME_FORMAT).replace(tzinfo=timezone.utc)

def datetime_to_str(dt: datetime) -> str:
    if not isinstance(dt, datetime):
        raise ValueError(f"Invalid type for dt: {dt} has type '{type(dt)}', expected datetime")
    return dt.strftime(SchedulerJobInfo.DATETIME_FORMAT)

def str_to_timedelta(string_value: str) -> timedelta:
    if not isinstance(string_value, str):
        raise ValueError(f"Invalid type for string_value: {string_value} has type '{type(string_value)}', expected str")

    if string_value == 'UNLIMITED':
        return None
    elif string_value == '0':
        return timedelta(0)

    values = string_value.split(':')
    seconds = float(values.pop())
    minutes = int(values.pop())
    hours, days = 0, 0
    if values:
        values = values.pop().split('-')
        hours = int(values.pop())
    if values:
        days = int(values.pop())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def timedelta_to_string(td: timedelta) -> str:
    if not isinstance(td, timedelta):
        raise ValueError(f"Invalid type for td: {td} has type '{type(td)}', expected datetime")
    days, seconds = td.days, td.seconds
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days:02d}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
