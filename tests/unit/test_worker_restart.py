import unittest
from types import SimpleNamespace
from unittest.mock import patch, call

from Common.py.models import VideoKey
from Matcher import main as worker


class WorkerRestartTests(unittest.TestCase):
    @patch.object(worker, 'setup_logging')
    @patch.object(worker.Config, 'initialize_from_ssm')
    @patch.object(worker, 'load_dotenv')
    @patch.object(worker.sqs_client, 'wait_for_notification')
    @patch.object(worker, '_process_videos')
    @patch.object(worker.animan_client, 'get_videos_to_match')
    def test_exits_only_after_draining_processed_work(self, get_videos, process_videos,
                                                    wait_for_notification, *_):
        first = [VideoKey(1, 'en', 1)]
        second = [VideoKey(2, 'en', 1)]
        get_videos.side_effect = [SimpleNamespace(videos_to_match=videos)
                                  for videos in ([], first, second, [])]

        worker.main()

        self.assertEqual(get_videos.call_count, 4)
        wait_for_notification.assert_called_once_with()
        self.assertEqual(process_videos.call_args_list,
                         [call(first, force_process_all_season=False),
                          call(second, force_process_all_season=False)])

    @patch.object(worker, 'setup_logging')
    @patch.object(worker.Config, 'initialize_from_ssm')
    @patch.object(worker, 'load_dotenv')
    @patch.object(worker.sqs_client, 'wait_for_notification', side_effect=KeyboardInterrupt)
    @patch.object(worker, '_process_videos')
    @patch.object(worker.animan_client, 'get_videos_to_match',
                  return_value=SimpleNamespace(videos_to_match=[]))
    def test_stays_running_when_no_work_has_arrived(self, get_videos, process_videos,
                                                    wait_for_notification, *_):
        worker.main()

        get_videos.assert_called_once_with()
        wait_for_notification.assert_called_once_with()
        process_videos.assert_not_called()


if __name__ == '__main__':
    unittest.main()
