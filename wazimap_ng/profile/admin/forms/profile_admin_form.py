from django import forms
from django.contrib.auth.models import Group

from ... import models
from wazimap_ng.admin_utils import GroupPermissionWidget

class ProfileAdminForm(forms.ModelForm):
	group_permissions = forms.ModelMultipleChoiceField(queryset=Group.objects.all(), required=False, widget=GroupPermissionWidget)

	class Meta:
		model = models.Profile
		widgets = {
			'permission_type': forms.RadioSelect,
		}
		fields = '__all__'


	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.fields["group_permissions"].widget.init_parameters(self.current_user, self.instance, self.instance.permission_type)