# Generated by Django 2.2.10 on 2020-04-07 06:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0061_auto_20200406_0255'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='profilehighlight',
            name='value',
        ),
        migrations.AddField(
            model_name='profilehighlight',
            name='subindicator',
            field=models.PositiveSmallIntegerField(blank=True, max_length=60),
        ),
    ]