# -*- coding: utf-8 -*-

import json

from pyehr.utils import decode_dict, cleanup_json

class ArchetypeBuilder(object):

    def __init__(self, archetype_dir):
        self.archetype_dir = archetype_dir

    def _get_quantity(self, value, units):
        return {
            'magnitude': value,
            'units': units
        }

    def _get_dv_text(self, text):
        return {
            'value' : text
        }

    def _get_dv_coded_text(self, coded_text_value,  code_string):
        return {
            'value' : coded_text_value,
            'defining_code' : {
                'terminology_id' : {
                    'value' : 'local::'
                },
            'code_string' : code_string
            }
        }

    def _clean_archetype(self, archetype):
        cleaned = cleanup_json(archetype)
        return decode_dict(cleaned)

    def _get_dv_duration(self, value, unit):
        return '%s%s' %(value, unit)

    def _get_dv_date_time(self, datetime_str): #todo: check format
        return datetime_str

    def _get_dv_multimedia(self, media_type, size):
        return {
                'media_type' : media_type,
                'size' : size
        }

    def _get_dv_proportion(self, proportion, value):
        return {
                "numerator" : proportion['numerator'],
                "denominator" : proportion['denominator'],
                "type" : {
                    "value" : value
                    }
        }

    def _load_file(self, archetype_id):
        path = '/'.join([self.archetype_dir, archetype_id])
        with open("%s.json" % path) as f:
            return json.loads(f.read())


    def build_blood_pressure_data(self, systolic=None, dyastolic=None, mean_arterial=None, pulse=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.blood_pressure.v1'
        bp_doc = self._load_file(archetype_id)
        try:
            bp_doc = bp_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        else:
            bp_doc = decode_dict(bp_doc)

        if systolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0004'] =\
                {'value': self._get_quantity(systolic, 'mm[Hg]')}
        if dyastolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0005'] =\
                {'value': self._get_quantity(dyastolic, 'mm[Hg]')}
        if mean_arterial:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1006'] =\
                {'value': self._get_quantity(mean_arterial, 'mm[Hg]')}
        if pulse:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1007'] =\
                {'value': self._get_quantity(pulse, 'mm[Hg]')}
        return archetype_id, self._clean_archetype(bp_doc)


    def build_blood_glucose_data(self, test_name=None, specimen_arch_detail=None, diet_intake=None, diet_duration=None, glucose_dose=None, glucose_timing=None, \
                            insulin_dose=None, insulin_route = None, laboratory_result_id = None, result_datetime= None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-blood_glucose.v1'
        bg_doc = self._load_file(archetype_id)
        try:
            bg_doc = bg_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)

        if test_name:
            bg_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(test_name)}
        if specimen_arch_detail: #todo: decide about  handling an example of nested archetipe
            pass
        if diet_intake:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.90'][0]['items']['at0.91'] =\
                {"value" : self._get_dv_coded_text(diet_intake,'at0.92' )}
        if diet_duration:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.90'][0]['items']['at0.96'] =\
                {'value' : self._get_dv_duration(diet_duration, 'H')}
        if glucose_dose:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.98'][0]['items']['at0.100'] =\
                {'value' : self._get_quantity(glucose_dose, 'gm')}
        if glucose_timing:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.98'][0]['items']['at0.99'] = \
                {'value': self._get_dv_coded_text(glucose_timing, 'at0.103')}
        if insulin_dose:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.107'][0]['items']['at0.110'] = \
                {'value' : self._get_quantity(insulin_dose, 'U')}
        if insulin_route:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.107'][0]['items']['at0.111'] = \
                {'value' : self._get_dv_coded_text(insulin_route, 'at0.112')}
        if laboratory_result_id:
            bg_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            bg_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(bg_doc)

    def build_full_blood_count_data(self, test_name=None, haemoglobin=None, mchc=None, mcv=None, mch=None, lymphocytes=None, basophils=None, \
                                    monocytes=None, eosinophils=None, multimedia_representation=None, laboratory_result_id = None, result_datetime= None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-full_blood_count.v1'
        fbc_doc = self._load_file(archetype_id)
        try:
            fbc_doc = fbc_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)

        if test_name:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(test_name)}
        if haemoglobin:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(haemoglobin, 'gm/l')}
        if mchc:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(mchc, 'gm/l')}
        if mcv:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.8'] = \
                {'value' : self._get_quantity(mcv, 'fl')}
        if lymphocytes:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.16]'] = \
                {'value' : self._get_quantity(lymphocytes, '10*9/l')}
        if basophils:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.17]'] = \
                {'value' : self._get_quantity(basophils, '10*9/l')}
        if monocytes:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.18]'] = \
                {'value' : self._get_quantity(monocytes, '10*9/l')}
        if eosinophils:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.19]'] = \
                {'value' : self._get_quantity(eosinophils, '10*9/l')}
        if multimedia_representation:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                [{'value' : self._get_dv_multimedia(multimedia_representation['media_type'], multimedia_representation['size'])}]
        if laboratory_result_id:
            fbc_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            fbc_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(fbc_doc)

    def build_lipids_data (self, test_name=None, specimen_detail=None, total_cholesterol=None, tryglicerides=None, hdl=None, ldl=None,  \
                           hdl_ldl_ratio=None, laboratory_result_id = None, result_datetime= None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-lipids.v1'
        lpd_doc = self._load_file(archetype_id)
        try:
            lpd_doc = lpd_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        if test_name:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(test_name)}
        if specimen_detail:
            pass  #decide about  handling an example of nested archetipe
        if total_cholesterol:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.5'] = \
                {'value' : self._get_quantity(total_cholesterol, "mg/dl")}
        if tryglicerides:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(tryglicerides, "mg/dl")}
        if hdl:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(hdl, "mg/dl")}
        if ldl:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(ldl, "mg/dl")}
        if hdl_ldl_ratio:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.1'] = \
                {'value' : self._get_dv_proportion(hdl_ldl_ratio, 1)}
        if laboratory_result_id:
            lpd_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            lpd_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(lpd_doc)

    def build_liver_function_data(self, test_name=None, alp=None, total_bilirubin=None, direct_bilirubin=None, indirect_bilirubin=None, alt=None, \
                                  ast=None, ggt=None, albumin=None, total_protein=None, laboratory_result_id = None, result_datetime= None) :
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-liver_function.v1'
        lvf_doc = self._load_file(archetype_id)
        try:
            lvf_doc = lvf_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        if test_name:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(test_name)}
        if alp:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.2'] =\
                {'value' : self._get_quantity(alp, "U/l")}
        if total_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.4'] =\
                {'value' : self._get_quantity(total_bilirubin, "µmol/l")}
        if direct_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.11'] =\
                {'value' : self._get_quantity(direct_bilirubin, "µmol/l")}
        if indirect_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.9'] =\
                {'value' : self._get_quantity(indirect_bilirubin, "µmol/l")}
        if alt:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.1'] =\
                {'value' : self._get_quantity(alt, "U/l")}
        if ast:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.3'] = \
                {'value' : self._get_quantity(ast, "U/l")}
        if ggt:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.3'] = \
                {'value' : self._get_quantity(ggt, "U/l")}
        if albumin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.7'] = \
                {'value' : self._get_quantity(albumin, "gm/l")}
        if total_protein:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.10'] = \
                {'value' : self._get_quantity(total_protein, "gm/l")}
        if laboratory_result_id:
            lvf_doc['protocol']['at0004'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            lvf_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(lvf_doc)

    def build_thyroid_data(self, test_name=None, tsh=None, ft3= None, total_t3=None, ft4=None, total_t4=None, ft3_index=None, fti=None, \
                           placer_id=None, filler_id=None, laboratory_result_id=None, result_datetime=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-thyroid.v1'
        thy_doc = self._load_file(archetype_id)
        try:
            thy_doc = thy_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        if test_name:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(test_name)}
        if tsh:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(tsh, 'mIU/l')}
        if ft3:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(ft3, 'pmol/l')}
        if total_t3:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.8'] = \
                {'value' : self._get_quantity(total_t3, 'pmol/l')}
        if ft4:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(ft3, 'pmol/l')}
        if total_t4:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(ft3, 'pmol/l')}
        if ft3_index:
             thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.9'] = \
                 {'value' : self._get_dv_proportion(ft3_index, 1)}
        if fti:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.6'] = \
                 {'value' : self._get_dv_proportion(fti, 1)}
        if placer_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0062'] = {'value' : placer_id}
        if filler_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0063'] = {'value' : filler_id}
        if laboratory_result_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            thy_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(thy_doc)

    def build_uae_data(self, test_name=None, sodum=None, potassium=None, chloride=None, bicarbonate=None, urea=None, creatinine=None, \
                       sp_ratio=None, laboratory_result_id=None, result_datetime=None):
        archetype_id  = 'openEHR-EHR-OBSERVATION.lab_test-urea_and_electrolytes.v1'
        uae_doc = self._load_file(archetype_id)
        try:
            uae_doc = uae_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        if test_name:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] = \
                {'value' : self._get_dv_text(test_name)}
        if sodum:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.5'] = \
                {'value' : self._get_quantity(sodum, 'mmol/l')}
        if potassium:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(potassium, 'mmol/l')}
        if chloride:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(chloride, 'mmol/l')}
        if bicarbonate:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(bicarbonate, 'mmol/l')}
        if urea:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.1'] = \
                {'value' : self._get_quantity(bicarbonate, 'mmol/l')}
        if creatinine:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(bicarbonate, 'mmol/l')}
        if sp_ratio:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.6'] = \
                {'value' : self._get_dv_proportion(sp_ratio, 1)}
        if laboratory_result_id:
            uae_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(laboratory_result_id)}
        if result_datetime:
            uae_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(result_datetime)}
        return archetype_id, self._clean_archetype(uae_doc)

    def build_urin_analysis_data(self, glucose=None, protein=None, bilirubin=None, ketones=None, blood=None, ph=None, comments=None ):
        archetype_id  = 'openEHR-EHR-OBSERVATION.urinalysis.v1'
        ualy_doc = self._load_file(archetype_id)
        try:
            ualy_doc = ualy_doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % archetype_id)
        if glucose:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0050'] = \
                {'value' : glucose}
        if protein:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0095'] = \
                {'value' : protein}
        if bilirubin:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0062'] = \
                {'value' : bilirubin}
        if ketones:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0037'] = \
                {'value' : ketones}
        if blood:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0032'] = \
                {'value' : blood}
        if ph:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0126'] = \
                {'value' : ph}
        if comments:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0030'] = \
                {'value' : self._get_dv_text(comments)}
        return archetype_id, self._clean_archetype(ualy_doc)