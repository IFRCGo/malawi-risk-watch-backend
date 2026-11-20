import typing

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

from apps.users.models import User


class Command(BaseCommand):
    help = "Create (or update) a user."

    @typing.override
    def add_arguments(self, parser: CommandParser):
        parser.add_argument("--name", type=str, required=True, help="Full name")
        parser.add_argument("--email", type=str, required=True, help="Email (login)")
        parser.add_argument("--password", type=str, required=True, help="Password")
        parser.add_argument("--role", type=str, required=True, choices=User.Role.values, help="User role")
        parser.add_argument("--superuser", action="store_true", help="Set is_superuser")
        parser.add_argument("--staff", action="store_true", help="Set is_staff")

    @typing.override
    @transaction.atomic
    def handle(self, *args: typing.Any, **options: typing.Any):
        user, created = User.objects.get_or_create(email=options["email"])
        user.name = options["name"]
        user.role = options["role"]
        user.is_superuser = options["superuser"]
        user.is_staff = options["staff"]
        user.is_active = True
        user.set_password(options["password"])
        user.save()

        action = "Created" if created else "Updated"
        self.stdout.write(self.style.SUCCESS(f"{action} user {user.email}"))
