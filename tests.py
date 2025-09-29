from __future__ import annotations

from django.db import connection, models
from django.test import TestCase

from merge_manager.config import FieldMergeRule, MergeProfile, registry
from merge_manager.exceptions import MergeValidationError
from merge_manager.models import MergeOperation
from merge_manager.services import MergeService
from merge_manager.settings import merge_manager_settings


class MergeServiceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        class Tag(models.Model):
            name = models.CharField(max_length=50)

            class Meta:
                app_label = 'merge_manager'

            def __str__(self) -> str:  # pragma: no cover - debug helper
                return self.name

        class City(models.Model):
            name = models.CharField(max_length=100)
            code = models.CharField(max_length=16, blank=True)
            population = models.IntegerField(default=0)
            is_active = models.BooleanField(default=True)
            tags = models.ManyToManyField('Tag', related_name='cities')

            class Meta:
                app_label = 'merge_manager'

            def __str__(self) -> str:  # pragma: no cover - debug helper
                return self.name

        class Office(models.Model):
            title = models.CharField(max_length=100)
            city = models.ForeignKey(City, on_delete=models.CASCADE, related_name='offices')

            class Meta:
                app_label = 'merge_manager'

            def __str__(self) -> str:  # pragma: no cover - debug helper
                return self.title

        cls.Tag = Tag
        cls.City = City
        cls.Office = Office

        with connection.schema_editor() as schema_editor:
            schema_editor.create_model(Tag)
            schema_editor.create_model(City)
            schema_editor.create_model(Office)

        merge_manager_settings.reload()
        registry.clear()
        cls.profile = MergeProfile(
            label='city',
            model=City,
            fields={
                'name': FieldMergeRule(strategy='prefer_target'),
                'code': FieldMergeRule(strategy='prefer_non_null'),
                'population': FieldMergeRule(strategy='prefer_donor'),
            },
        )
        registry.register(cls.profile)

    @classmethod
    def tearDownClass(cls):
        try:
            with connection.schema_editor() as schema_editor:
                schema_editor.delete_model(cls.Office)
                schema_editor.delete_model(cls.City)
                schema_editor.delete_model(cls.Tag)
        finally:
            registry.clear()
            super().tearDownClass()

    def setUp(self) -> None:
        self.service = MergeService(self.profile)

    def test_merge_moves_fields_and_relations(self):
        tag_a = self.Tag.objects.create(name='A')
        tag_b = self.Tag.objects.create(name='B')

        target = self.City.objects.create(name='Target', code='', population=100)
        donor = self.City.objects.create(name='Donor', code='DN', population=250)
        target.tags.add(tag_a)
        donor.tags.add(tag_b)
        office = self.Office.objects.create(title='Branch', city=donor)

        result = self.service.merge(
            target=target,
            donor=donor,
            field_overrides={'name': 'Merged City'},
            extra_summary={'trigger': 'unit-test'},
        )

        target.refresh_from_db()
        donor.refresh_from_db()
        office.refresh_from_db()

        self.assertEqual(target.name, 'Merged City')
        self.assertEqual(target.population, 250)
        self.assertEqual(target.code, 'DN')
        self.assertFalse(donor.is_active)
        self.assertEqual(office.city_id, target.pk)

        self.assertSetEqual(set(target.tags.values_list('name', flat=True)), {'A', 'B'})
        self.assertEqual(donor.tags.count(), 0)

        self.assertEqual(result.status, 'completed')
        self.assertIn('name', result.changed_fields)
        self.assertEqual(result.changed_fields['name']['source'], 'override')
        self.assertIn('offices', result.relations)
        self.assertEqual(result.relations['offices']['count'], 1)
        self.assertEqual(result.soft_delete['field'], 'is_active')
        self.assertFalse(result.soft_delete['dry_run'])

        audit = result.audit_record
        self.assertIsNotNone(audit)
        self.assertEqual(audit.profile, 'city')
        self.assertFalse(audit.dry_run)
        self.assertEqual(audit.status, 'completed')
        self.assertEqual(audit.summary['extra']['trigger'], 'unit-test')
        self.assertEqual(MergeOperation.objects.count(), 1)

    def test_dry_run_does_not_mutate_database(self):
        tag = self.Tag.objects.create(name='Z')
        target = self.City.objects.create(name='Base', code='', population=10)
        donor = self.City.objects.create(name='Donor', code='DN', population=20)
        donor.tags.add(tag)
        self.Office.objects.create(title='HQ', city=donor)

        result = self.service.merge(target=target, donor=donor, dry_run=True)

        target.refresh_from_db()
        donor.refresh_from_db()

        self.assertEqual(target.name, 'Base')
        self.assertEqual(target.population, 10)
        self.assertTrue(donor.is_active)
        self.assertEqual(target.tags.count(), 0)
        self.assertEqual(donor.tags.count(), 1)

        self.assertEqual(result.status, 'dry_run')
        self.assertTrue(result.dry_run)
        self.assertEqual(result.relations['tags']['count'], 1)
        self.assertTrue(result.soft_delete['dry_run'])

        audit = result.audit_record
        self.assertIsNotNone(audit)
        self.assertTrue(audit.dry_run)
        self.assertEqual(audit.status, 'dry_run')

    def test_hard_delete_removes_donor(self):
        target = self.City.objects.create(name='Target HD', code='', population=5)
        donor = self.City.objects.create(name='Donor HD', code='HD', population=15)

        profile = MergeProfile(
            label='city-hard-delete',
            model=self.City,
            fields={
                'name': FieldMergeRule(strategy='prefer_target'),
                'code': FieldMergeRule(strategy='prefer_non_null'),
                'population': FieldMergeRule(strategy='prefer_donor'),
            },
            hard_delete=True,
        )

        service = MergeService(profile)

        donor_pk = donor.pk
        result = service.merge(target=target, donor=donor)

        target.refresh_from_db()

        self.assertTrue(result.hard_delete.get('enabled'))
        self.assertTrue(result.hard_delete.get('applied'))
        self.assertFalse(result.dry_run)
        self.assertEqual(result.soft_delete.get('reason'), 'hard_delete_enabled')

        with self.assertRaises(self.City.DoesNotExist):
            self.City.objects.get(pk=donor_pk)

        audit = result.audit_record
        self.assertIsNotNone(audit)
        self.assertTrue(audit.summary['hard_delete']['enabled'])
        self.assertTrue(audit.summary['hard_delete']['applied'])

    def test_validation_rejects_wrong_type(self):
        donor = self.City.objects.create(name='Donor')
        with self.assertRaises(MergeValidationError):
            self.service.merge(target=object(), donor=donor)
