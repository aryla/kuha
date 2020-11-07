import unittest
from datetime import datetime
import logging

import mock

from ..test_models import ModelTestCase
from ..util import LogCapture
from ...exception import HarvestError
from ...importer import harvest

def make_item(identifier):
    item = mock.Mock()
    item.identifier = identifier
    return item


def make_format(prefix):
    format_ = mock.Mock()
    format_.prefix = prefix
    return format_


class TestUpdateFormats(ModelTestCase):

    def test_successful_update(self):
        formats = {
            'oai_dc': (
                'http://www.openarchives.org/OAI/2.0/oai_dc/',
                'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
            ),
            'ddi': (
                'http://www.icpsr.umich.edu/DDI/Version2-0',
                'http://www.icpsr.umich.edu/DDI/Version2-0.dtd',
            ),
        }

        oai_dc_mock = make_format('oai_dc')
        ead_mock = make_format('ead')

        provider = mock.Mock()
        provider.formats.return_value = formats

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                models.Format.list.return_value = [oai_dc_mock, ead_mock]
                new_prefixes = harvest.update_formats(provider, purge=True)

        self.assertCountEqual(new_prefixes, list(formats.keys()))
        self.assertEqual(oai_dc_mock.mark_as_deleted.mock_calls, [])
        ead_mock.mark_as_deleted.assert_called_once_with()
        models.Format.list.assert_called_once_with(ignore_deleted=True)
        self.assertCountEqual(
            models.Format.create_or_update.mock_calls,
            [mock.call(p, n, s) for p, (n, s) in formats.items()]
        )
        models.purge_deleted.assert_called_once_with()
        provider.formats.assert_called_once_with()
        models.commit.assert_called_once_with()

        log.assert_emitted('Removed 1 format and added 1 format.')

    def test_no_formats(self):
        provider = mock.Mock()
        provider.formats.return_value = {}
        with self.assertRaises(HarvestError) as cm:
            harvest.update_formats(provider)
        self.assertIn('no formats', str(cm.exception))

    def test_provider_fails(self):
        provider = mock.Mock()
        provider.formats.side_effect = ImportError('some message')

        with LogCapture(harvest) as log:
            with self.assertRaises(HarvestError) as cm:
                harvest.update_formats(provider)

        self.assertIn('some message', str(cm.exception))
        log.assert_emitted('Failed to update metadata formats')

    def test_invalid_format(self):
        provider = mock.Mock()
        provider.formats.return_value = {'prefix': 'invalid'}
        self.assertRaises(HarvestError, harvest.update_formats, provider)

    def test_dry_run(self):
        provider = mock.Mock()
        provider.formats.return_value = {'oai_dc': ('namespace', 'schema')}

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                models.Format.list.return_value = []
                harvest.update_formats(provider, purge=True, dry_run=True)

        self.assertEqual(models.Format.create_or_update.mock_calls, [])
        self.assertEqual(models.purge_deleted.mock_calls, [])
        self.assertEqual(models.commit.mock_calls, [])

        log.assert_emitted('Removed 0 formats and added 1 format.')


class TestUpdateItems(ModelTestCase):

    def test_successful_update(self):
        identifiers = ['asd', 'U', 'a:b']
        provider = mock.Mock()
        provider.identifiers.return_value = identifiers
        item_mocks = [make_item('1234'), make_item('asd')]

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                models.Item.list.return_value = item_mocks
                new_ids = harvest.update_items(provider, purge=True)

        self.assertCountEqual(new_ids, identifiers)
        provider.identifiers.assert_called_once_with()
        item_mocks[0].mark_as_deleted.assert_called_once_with()
        self.assertEqual(item_mocks[1].mark_as_deleted.mock_calls, [])
        self.assertCountEqual(
            models.Item.create_or_update.mock_calls,
            [mock.call(i) for i in ['asd', 'U', 'a:b']]
        )
        models.purge_deleted.assert_called_once_with()
        models.commit.assert_called_once_with()
        log.assert_emitted('Removed 1 item and added 2 items.')

    def test_no_identifiers(self):
        provider = mock.Mock()
        provider.identifiers.return_value = []
        item_mock = make_item('id')

        with mock.patch.object(harvest, 'models') as models:
            models.Item.list.return_value = [item_mock]
            harvest.update_items(provider, purge=False)
        item_mock.mark_as_deleted.assert_called_once_with()

    def test_provider_fails(self):
        provider = mock.Mock()
        provider.identifiers.side_effect = ValueError('abcabc')

        with self.assertRaises(HarvestError) as cm:
            harvest.update_items(provider)
        self.assertIn('abcabc', str(cm.exception))

    def test_duplicate_identifiers(self):
        provider = mock.Mock()
        provider.identifiers.return_value = [
            'i2', 'i1', 'i3', 'i1', 'i1', 'i2',
        ]

        with mock.patch.object(harvest, 'models') as models:
            models.Item.list.return_value = []
            new_ids = harvest.update_items(provider, purge=False)
        self.assertCountEqual(
            models.Item.create_or_update.mock_calls,
            [mock.call(i) for i in ['i1', 'i2', 'i3']]
        )
        self.assertCountEqual(new_ids, ['i1', 'i2', 'i3'])

    def test_invalid_identifiers(self):
        class InvalidId(object):
            def __str__(self):
                raise TypeError('conversion failed')
        provider = mock.Mock()
        provider.identifiers.return_value = [
            'ok', InvalidId(), 'oai:1234',
        ]

        with mock.patch.object(harvest, 'models') as models:
            models.Item.list.return_value = []
            with self.assertRaises(HarvestError) as cm:
                harvest.update_items(provider)
        self.assertIn('conversion failed', str(cm.exception))

    def test_dry_run(self):
        provider = mock.Mock()
        provider.identifiers.return_value = ['asd']
        item_mock = make_item('1234')

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                models.Item.list.return_value = [item_mock]
                harvest.update_items(provider, purge=True, dry_run=True)

        self.assertEqual(models.Item.create_or_update.mock_calls, [])
        self.assertEqual(models.purge_deleted.mock_calls, [])
        self.assertEqual(models.commit.mock_calls, [])
        self.assertEqual(item_mock.mark_as_deleted.mock_calls, [])

        log.assert_emitted('Removed 1 item and added 1 item.')


class TestUpdateRecords(ModelTestCase):

    def test_successful_harvest(self):
        prefixes = ['ead', 'oai_dc']
        identifiers = ['item{0}'.format(i) for i in range(4)]
        time = datetime(2014, 2, 4, 10, 54, 27)

        provider = mock.Mock()
        provider.get_record.return_value = '<xml ... />'
        provider.has_changed.side_effect = (
            lambda identifier, _: identifier != 'item2'
        )

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                with mock.patch.object(harvest, 'update_sets') as (
                        update_sets_mock):
                    harvest.update_records(
                        provider, identifiers, prefixes, time)

        self.assertCountEqual(
            provider.get_record.mock_calls,
            [mock.call(id_, prefix)
             for id_ in ['item0', 'item1', 'item3']
             for prefix in ['ead', 'oai_dc']]
        )
        self.assertCountEqual(
            update_sets_mock.mock_calls,
            [mock.call(provider, id_, False)
             for id_ in ['item0', 'item1', 'item3']],
        )
        self.assertCountEqual(
            models.Record.create_or_update.mock_calls,
            [mock.call(id_, prefix, '<xml ... />')
             for id_ in ['item0', 'item1', 'item3']
             for prefix in ['ead', 'oai_dc']]
        )
        self.assertEqual(
            models.commit.mock_calls,
            [mock.call() for _ in range(6)]
        )
        log.assert_emitted('Skipping item "item2"')
        log.assert_emitted('Updated 6 records.')

    def test_no_time(self):
        prefixes = ['oai_dc']
        items = ['oai:test:id']
        provider = mock.Mock()
        provider.get_record.return_value = '<oai_dc:dc>...</oai_dc:dc>'

        with mock.patch.object(harvest, 'update_sets'):
            with mock.patch.object(harvest, 'models') as models:
                harvest.update_records(
                    provider, items, prefixes, since=None)

        self.assertEqual(provider.has_changed.mock_calls, [])
        provider.get_record.assert_called_once_with(
            'oai:test:id', 'oai_dc')

    def test_no_records(self):
        provider = mock.Mock()
        time = datetime(2014, 2, 4, 10, 54, 27)
        with mock.patch.object(harvest, 'models') as models:
            harvest.update_records(provider, [], ['ead'], since=time)
        self.assertEqual(provider.has_changed.mock_calls, [])
        self.assertEqual(provider.get_record.mock_calls, [])
        self.assertEqual(models.commit.mock_calls, [])

    def test_harvest_fails(self):
        items = ['id1', 'id2']
        xml = 'data'

        def get_record(id_, prefix):
            if id_ == 'id1':
                raise ValueError('crosswalk error')
            else:
                return xml
        provider = mock.Mock()
        provider.get_record.side_effect = get_record

        with mock.patch.object(harvest, 'update_sets'):
            with mock.patch.object(harvest, 'models') as models:
                with LogCapture(harvest) as log:
                    harvest.update_records(provider, items, ['ead'])

        models.Record.create_or_update.assert_called_once_with(
            'id2', 'ead', xml)
        log.assert_emitted(
            'Failed to disseminate format "ead" for item "id1"')
        log.assert_emitted('crosswalk error')

    def test_deleted_record(self):
        provider = mock.Mock()
        provider.get_record.return_value = None

        with mock.patch.object(harvest, 'update_sets'):
            with mock.patch.object(harvest, 'models') as models:
                harvest.update_records(
                    provider, ['some_item'], ['oai_dc'])

        models.Record.mark_as_deleted.assert_called_once_with(
            'some_item', 'oai_dc',
        )

    def test_update_sets_fails(self):
        items = ['item1', 'item2']

        provider = mock.Mock()
        provider.get_record.return_value = '<oai_dc:dc>...</oai_dc:dc>'

        with mock.patch.object(harvest, 'update_sets') as (
                update_sets_mock):
            update_sets_mock.side_effect = ValueError('invalid set spec')
            with mock.patch.object(harvest, 'models') as models:
                with LogCapture(harvest) as log:
                    harvest.update_records(provider, items, ['oai_dc'])

        self.assertCountEqual(
            update_sets_mock.mock_calls,
            [mock.call(provider, id_, False)
             for id_ in ['item1', 'item2']],
        )
        log.assert_emitted('Failed to update item "item1"')
        log.assert_emitted('Failed to update item "item2"')
        log.assert_emitted('invalid set spec')

    def test_delete_single_record(self):
        formats = ['oai_dc', 'ead', 'ddi']
        def get_record(id_, prefix):
            if prefix == 'oai_dc':
                raise ValueError('invalid data')
            elif prefix == 'ead':
                return None
            elif prefix == 'ddi':
                return 'data'
        provider = mock.Mock()
        provider.get_record.side_effect = get_record
        provider.get_sets.return_value = []

        with mock.patch.object(harvest, 'models') as models:
            harvest.update_records(provider, ['pelle'], formats)

        models.Record.mark_as_deleted.assert_called_once_with(
            'pelle', 'ead')
        models.Record.create_or_update.assert_called_once_with(
            'pelle', 'ddi', 'data')

    def test_dry_run(self):
        time = datetime(2014, 2, 4, 10, 54, 27)

        provider = mock.Mock()
        provider.get_record.return_value = '<xml ... />'
        provider.has_changed.return_value = True

        with LogCapture(harvest) as log:
            with mock.patch.object(harvest, 'models') as models:
                with mock.patch.object(harvest, 'update_sets') as (
                        update_sets_mock):
                    harvest.update_records(
                        provider,
                        ['item1'],
                        ['oai_dc'],
                        time,
                        dry_run=True,
                    )

        update_sets_mock.assert_called_once_with(provider, 'item1', True)
        self.assertEqual(models.Record.create_or_update.mock_calls, [])
        self.assertEqual(models.commit.mock_calls, [])

        log.assert_emitted('Updated 1 record.')


class TestUpdateSets(ModelTestCase):

    def test_valid_sets(self):
        provider = mock.Mock()
        provider.get_sets.return_value = [
            ('a:b', 'Set B'),
            ('a',   'Set A'),
            ('a:b:c','Set C'),
        ]

        with mock.patch.object(harvest, 'models') as models:
            harvest.update_sets(provider, 'oai:example.org:item')

        models.Item.get.assert_called_once_with('oai:example.org:item')
        item = models.Item.get.return_value
        item.clear_sets.assert_called_once_with()
        self.assertEqual(
            models.Set.create_or_update.mock_calls,
            [mock.call('a', 'Set A'),
             mock.call('a:b', 'Set B'),
             mock.call('a:b:c', 'Set C')]
        )
        set_ = models.Set.create_or_update.return_value
        self.assertEqual(
            item.add_to_set.mock_calls,
            [mock.call(set_) for _ in range(3)]
        )

    def test_no_sets(self):
        provider = mock.Mock()
        provider.get_sets.return_value = []
        with mock.patch.object(harvest, 'models') as models:
            harvest.update_sets(provider, 'item')
        item = models.Item.get.return_value
        item.clear_sets.assert_called_once_with()
        self.assertEqual(item.add_to_set.mock_calls, [])

    def test_dry_run(self):
        provider = mock.Mock()
        provider.get_sets.return_value = [('a', 'Set Name')]

        with mock.patch.object(harvest, 'models') as models:
            models.Item
            harvest.update_sets(
                provider,
                'oai:example.org:item',
                dry_run=True,
            )
        item_mock = models.Item.get.return_value

        self.assertEqual(item_mock.clear_sets.mock_calls, [])
        self.assertEqual(models.Set.create_or_update.mock_calls, [])
        self.assertEqual(item_mock.add_to_set.mock_calls, [])
