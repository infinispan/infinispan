Internationalising documentation
--------------------------------

Documentation for JBoss Cache is written using docbook XML.  There is a lot of reference material on docbook XML if
you are unfamiliar with it, but JBoss Cache's usage of docbook is pretty basic and most of it can be picked up by looking
through the existing documentation sources, which are in src/main/docbook.

For each document, the 'en' version is treated as the reference source for all translations.

Starting a new translation
--------------------------

** NB: This section needs updating since moving to subversion.

Each time a new translation is started, the docs directory should be updated and tagged as I18N_<2-letter iso lang code>_<2-letter iso country code> (optional)
such as I18N_IT or I18N_PT_BR (note all in upper case)

Updating translations
---------------------

Each time a new release is made, translations should be updated by:

1) Doing a diff of the 'en' docs, comparing the latest against the translation tag (e.g., I18N_IT)
2) Updating the translation (e.g., the 'it' docs)
3) Moving the translation tag on the 'en' files (e.g., I18N_IT) to the current snapshot.

- Manik Surtani

