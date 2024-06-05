# coding: utf-8

"""
    JobSet SDK

    Python SDK for the JobSet API  # noqa: E501

    The version of the OpenAPI document: v0.1.4
    Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six

from jobset.configuration import Configuration


class JobsetV1alpha2JobSetStatus(object):
    """NOTE: This class is auto generated by OpenAPI Generator.
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    openapi_types = {
        'conditions': 'list[V1Condition]',
        'phase': 'str',
        'replicated_jobs_status': 'list[JobsetV1alpha2ReplicatedJobStatus]',
        'restarts': 'int',
        'restarts_count_towards_max': 'int'
    }

    attribute_map = {
        'conditions': 'conditions',
        'phase': 'phase',
        'replicated_jobs_status': 'replicatedJobsStatus',
        'restarts': 'restarts',
        'restarts_count_towards_max': 'restartsCountTowardsMax'
    }

    def __init__(self, conditions=None, phase=None, replicated_jobs_status=None, restarts=None, restarts_count_towards_max=None, local_vars_configuration=None):  # noqa: E501
        """JobsetV1alpha2JobSetStatus - a model defined in OpenAPI"""  # noqa: E501
        if local_vars_configuration is None:
            local_vars_configuration = Configuration()
        self.local_vars_configuration = local_vars_configuration

        self._conditions = None
        self._phase = None
        self._replicated_jobs_status = None
        self._restarts = None
        self._restarts_count_towards_max = None
        self.discriminator = None

        if conditions is not None:
            self.conditions = conditions
        if phase is not None:
            self.phase = phase
        if replicated_jobs_status is not None:
            self.replicated_jobs_status = replicated_jobs_status
        if restarts is not None:
            self.restarts = restarts
        if restarts_count_towards_max is not None:
            self.restarts_count_towards_max = restarts_count_towards_max

    @property
    def conditions(self):
        """Gets the conditions of this JobsetV1alpha2JobSetStatus.  # noqa: E501


        :return: The conditions of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :rtype: list[V1Condition]
        """
        return self._conditions

    @conditions.setter
    def conditions(self, conditions):
        """Sets the conditions of this JobsetV1alpha2JobSetStatus.


        :param conditions: The conditions of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :type: list[V1Condition]
        """

        self._conditions = conditions

    @property
    def phase(self):
        """Gets the phase of this JobsetV1alpha2JobSetStatus.  # noqa: E501

        Phase of the JobSet.  # noqa: E501

        :return: The phase of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :rtype: str
        """
        return self._phase

    @phase.setter
    def phase(self, phase):
        """Sets the phase of this JobsetV1alpha2JobSetStatus.

        Phase of the JobSet.  # noqa: E501

        :param phase: The phase of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :type: str
        """

        self._phase = phase

    @property
    def replicated_jobs_status(self):
        """Gets the replicated_jobs_status of this JobsetV1alpha2JobSetStatus.  # noqa: E501

        ReplicatedJobsStatus track the number of JobsReady for each replicatedJob.  # noqa: E501

        :return: The replicated_jobs_status of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :rtype: list[JobsetV1alpha2ReplicatedJobStatus]
        """
        return self._replicated_jobs_status

    @replicated_jobs_status.setter
    def replicated_jobs_status(self, replicated_jobs_status):
        """Sets the replicated_jobs_status of this JobsetV1alpha2JobSetStatus.

        ReplicatedJobsStatus track the number of JobsReady for each replicatedJob.  # noqa: E501

        :param replicated_jobs_status: The replicated_jobs_status of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :type: list[JobsetV1alpha2ReplicatedJobStatus]
        """

        self._replicated_jobs_status = replicated_jobs_status

    @property
    def restarts(self):
        """Gets the restarts of this JobsetV1alpha2JobSetStatus.  # noqa: E501

        Restarts tracks the number of times the JobSet has restarted (i.e. recreated in case of RecreateAll policy).  # noqa: E501

        :return: The restarts of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :rtype: int
        """
        return self._restarts

    @restarts.setter
    def restarts(self, restarts):
        """Sets the restarts of this JobsetV1alpha2JobSetStatus.

        Restarts tracks the number of times the JobSet has restarted (i.e. recreated in case of RecreateAll policy).  # noqa: E501

        :param restarts: The restarts of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :type: int
        """

        self._restarts = restarts

    @property
    def restarts_count_towards_max(self):
        """Gets the restarts_count_towards_max of this JobsetV1alpha2JobSetStatus.  # noqa: E501

        RestartsCountTowardsMax tracks the number of times the JobSet has restarted that counts towards the maximum allowed number of restarts.  # noqa: E501

        :return: The restarts_count_towards_max of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :rtype: int
        """
        return self._restarts_count_towards_max

    @restarts_count_towards_max.setter
    def restarts_count_towards_max(self, restarts_count_towards_max):
        """Sets the restarts_count_towards_max of this JobsetV1alpha2JobSetStatus.

        RestartsCountTowardsMax tracks the number of times the JobSet has restarted that counts towards the maximum allowed number of restarts.  # noqa: E501

        :param restarts_count_towards_max: The restarts_count_towards_max of this JobsetV1alpha2JobSetStatus.  # noqa: E501
        :type: int
        """

        self._restarts_count_towards_max = restarts_count_towards_max

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.openapi_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, JobsetV1alpha2JobSetStatus):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, JobsetV1alpha2JobSetStatus):
            return True

        return self.to_dict() != other.to_dict()
