from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
class LogAna(MRJob):
    def mapper(self, _, line):
        parts = line.strip().split()
        if len(parts) == 3:
            user, time, action = parts
            yield user, f"{time}\t{action}"
    def reducer_user_time(self, user, records):
        login_time = None
        total_seconds = 0
        for record in records:
            time_str, action = record.split()
            time = datetime.fromisoformat(time_str)
            if action == "login":
                login_time = time
            elif action == "logout" and login_time:
                total_seconds += (time - login_time).total_seconds()
                login_time = None
        yield None, (user, round(total_seconds / 3600, 2))
    def reducer_final(self, _, user_time_pairs):
        users = list(user_time_pairs)
        total_users = len(users)
        max_time = max(users, key=lambda x: x[1])[1]
        sorted_users = sorted(users, key=lambda x: x[1], reverse=True)
        top_users = [u for u, t in sorted_users if t == max_time]
        yield "LOGIN TIME ANALYSIS ", ""
        yield "Total Users", total_users
        yield "Maximum Login Time (hours)", max_time
        yield " All Users (sorted by login time)", ""
        for user, hours in sorted_users:
            yield user, f"{hours} hours"
        yield " Users with Maximum Login Time ", ""
        for user in top_users:
            yield user, f"{max_time} hours"
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_user_time),
            MRStep(reducer=self.reducer_final)
        ]
if __name__ == "__main__":
    LogAna.run()
