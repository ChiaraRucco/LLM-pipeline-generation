import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag

class TestETLDag(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder='my_etl_pipeline/dags', include_examples=False)

    def test_dag_import(self):
        self.assertFalse(len(self.dagbag.import_errors), "DAG import errors found")

    def test_fetch_data_task(self):
        dag = self.dagbag.get_dag('etl_dag')
        task = dag.get_task('fetch_data')
        self.assertIsNotNone(task)

    def test_transform_data_task(self):
        dag = self.dagbag.get_dag('etl_dag')
        task = dag.get_task('transform_data')
        self.assertIsNotNone(task)

    def test_post_data_task(self):
        dag = self.dagbag.get_dag('etl_dag')
        task = dag.get_task('post_data')
        self.assertIsNotNone(task)

    @patch('requests.get')
    def test_fetch_data(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = [{'id': 1, 'name': 'Test Category'}]
        mock_get.return_value = mock_response

        dag = self.dagbag.get_dag('etl_dag')
        task = dag.get_task('fetch_data')
        result = task.execute(context={})
        self.assertEqual(result, [{'id': 1, 'name': 'Test Category'}])

    @patch('requests.post')
    def test_post_data(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'success': True}
        mock_post.return_value = mock_response

        dag = self.dagbag.get_dag('etl_dag')
        task = dag.get_task('post_data')
        transformed_data = [{'id': 1, 'name': 'Test Category'}]
        task.execute(context={'ti': MagicMock(xcom_pull=MagicMock(return_value=transformed_data))})
        mock_post.assert_called_once_with('https://httpbin.org/post', json=transformed_data)

if __name__ == '__main__':
    unittest.main()