import sys
import os
from Deadline.Events import *
from Deadline.Scripting import RepositoryUtils
from Deadline.Scripting import JobUtils


if 'O:/pts/tools/render_stat' not in sys.path:
    sys.path.append('O:/pts/tools')
import render_stat.pgdb as pgdb
import render_stat.util as util

def GetDeadlineEventListener():
    return StatGather()


def CleanupDeadlineEventListener(plugin_to_be_cleaned):
    plugin_to_be_cleaned.Cleanup()


class StatGather(DeadlineEventListener):
    def __init__(self):
        self.OnJobSubmittedCallback += self.OnJobSubmitted
        self.OnJobFinishedCallback += self.OnJobFinished

    def Cleanup(self):
        del self.OnJobFinishedCallback

    def OnJobSubmitted(self, job):
        pass

    def OnJobFinished(self, job):
        print '=============== stat_gathering start ==============='

        # check plugin. If not Maya, ignore
        if 'maya' in job.JobPlugin.lower():
            print 'This is a Maya job.'

            # prepare data to store
            info = job.JobName.split('_') if '_' in job.JobName else []

            if len(info) >= 4:
                print 'Enough tokens in job name. Start gathering stats.'
                projects = util.read_json('project.json')

                if info[0] not in projects['projects']:
                    print 'Not in collecting projects. Skipping.'

                else:
                    jobname = job.JobName
                    jobid = job.JobId
                    project = info[0]
                    episode = info[1]
                    shot = '{}_{}'.format(info[2], info[3])
                    shot_unique = '{}_{}_{}'.format(project, episode, shot)
                    renderlayer = job.JobExtraInfo8
                    # convert version to int
                    version = job.JobExtraInfo9
                    version = int(version.replace('v', '') if version else 0)

                    stats = JobUtils.CalculateJobStatistics(job, RepositoryUtils.GetJobTasks(job, True))

                    rendertime = to_sec(stats.AverageFrameRenderTimeAsString)
                    errortime = to_sec(stats.WastedErrorTimeAsString)
                    peakram = to_gb(stats.PeakRamUsage)
                    framecount = stats.FrameCount
                    errorcount = stats.ErrorReports

                    print 'JobId = {}'.format(jobid)
                    print 'project = {}'.format(project)
                    print 'episode = {}'.format(episode)
                    print 'shot = {}'.format(shot)
                    print 'version = {}'.format(version)
                    print 'renderlayer = {}'.format(renderlayer)

                    print 'WastedErrorTime in seconds = {}'.format(errortime)
                    print 'rendertime in seconds = {}'.format(rendertime)
                    print 'PeakRamUsage = {} Gb'.format(peakram)
                    print 'FrameCount = {}'.format(framecount)
                    print 'ErrorReports (error count) = {}'.format(errorcount)
                    print ''

                    tokens = pgdb.get_connection_tokens()
                    try:
                        print 'Connecting to database @{}'.format(tokens['host'])
                        conn = pgdb.init_connection(**tokens)
                        print 'Connection success.'

                        curr = conn.cursor()

                        print 'Insert project'
                        curr.execute("""
                            INSERT INTO project (nameshort) VALUES (%s)
                            ON CONFLICT (nameshort) DO NOTHING""", (project,))
                        conn.commit()

                        print 'Insert episode'
                        curr.execute("""
                            INSERT INTO episode (nameshort, project_id)
                            VALUES (
                                %s,
                                (SELECT project_id FROM project WHERE nameshort=%s))
                            ON CONFLICT (nameshort) DO NOTHING""", (episode, project))
                        conn.commit()

                        print 'Insert shot'
                        curr.execute("""
                            INSERT INTO shot (nameshort, nameunique, episode_id)
                            VALUES (
                                %s, %s,
                                (SELECT episode_id FROM episode WHERE nameshort=%s))
                            ON CONFLICT (nameunique) DO NOTHING""",
                            (shot, shot_unique, episode))
                        conn.commit()

                        print 'Insert render job.'
                        arg = {
                                'job_id': jobid,
                                'version': version,
                                'renderlayer': renderlayer,
                                'errortime': errortime,
                                'errorcount': errorcount,
                                'rendertime': rendertime,
                                'peakram': peakram,
                                'framecount': framecount,
                                'shot_unique': shot_unique}
                        curr.execute("""
                            INSERT INTO shot_render_stat
                            (job_id, version, renderlayer, errortime, errorcount, rendertime, peakram, framecount, shot_id)
                            VALUES (
                                %(job_id)s, %(version)s, %(renderlayer)s, %(errortime)s, %(errorcount)s,
                                %(rendertime)s, %(peakram)s, %(framecount)s, 
                                (SELECT shot_id FROM shot WHERE nameunique=%(shot_unique)s))
                            ON CONFLICT (job_id) DO UPDATE SET
                                errortime = %(errortime)s, errorcount = %(errorcount)s,
                                rendertime = %(rendertime)s, peakram = %(peakram)s, timestamp = now()""", arg)
                        conn.commit()

                        curr.close()
                        conn.close()
                    except:
                        print 'Database processing error.'

            else:
                print 'not enough tokens in job name. Abort.'
        else:
            print 'Not a Maya job. Abort.'

        print '=============== stat_gather end ================='
        # deadlinePlugin.LogInfo('=============== end =================')


def to_sec(arg):
    x = arg.split('.')[0]
    y = map(int, x.split(':'))
    return sum(n * sec for n, sec in zip(y[::-1], (1, 60, 3600)))


def to_gb(b):
    return '{:.2f}'.format(float(b)/1073741824.0)
