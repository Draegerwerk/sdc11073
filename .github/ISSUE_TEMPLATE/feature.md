name: 🚀 Feature Request
description: File a new feature request
title: 'feature: <title>'
labels: [enhancement]
body:
  - type: markdown
    attributes:
      value: ':stop_sign: _If you want to talk about your feature request first, please visit the [ideas discussions](https://github.com/Draegerwerk/sdc11073/discussions/categories/ideas)._'
  - type: checkboxes
    attributes:
      label: 'Is there an existing issue for this?'
      description: 'Please [search :mag: the issues](https://github.com/Draegerwerk/sdc11073/issues) to check if this bug has already been reported.'
      options:
      - label: 'I have searched the existing issues'
        required: true
  - type: textarea
    attributes:
      label: 'Feature idea'
      description: 'Describe the feature idea in great detail.'
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Alternatives'
      description: 'Can you achieve the same result doing it in an alternative way? Is the alternative considerable? Why?'
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Alternatives'
      description: 'Can you achieve the same result doing it in an alternative way? Is the alternative considerable? Why?'
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Implementation idea'
      description: 'Do you already have an idea how this could be implemented?'
    validations:
      required: false
  - type: checkboxes
    attributes:
      label: Participation
      options:
        - label: I am willing to submit a pull request to implement this feature.
          required: false
  - type: input
    attributes:
      label: 'Link to the idea discussion'
      description: 'Provide the link to the discussion, if there is one.'
    validations:
      required: false