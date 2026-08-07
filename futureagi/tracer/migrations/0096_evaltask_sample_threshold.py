from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("tracer", "0095_merge_20260722_1400"),
    ]

    operations = [
        migrations.AddField(
            model_name="evaltask",
            name="sample_threshold",
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]
