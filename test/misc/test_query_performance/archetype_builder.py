# -*- coding: utf-8 -*-

import json
from random import randint, uniform, choice

from pyehr.utils import decode_dict, cleanup_json

class ArchetypeBuilder(object):

    def __init__(self, archetype_id, archetype_dir):
        self.archetype_id = archetype_id
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

    def _load_file(self):
        path = '/'.join([self.archetype_dir, self.archetype_id])
        with open("%s.json" % path) as f:
            doc = json.loads(f.read())

        try:
            doc = doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % self.archetype_id)
        else:
            doc = decode_dict(doc)

        return doc

    def build(self):
        raise NotImplementedError()


class BloodGlucose(ArchetypeBuilder):

    def __init__(self, archetype_dir, test_name=None, specimen_arch_detail=None, diet_intake=None,
                 diet_duration=None, glucose_dose=None, glucose_timing=None,
                 insulin_dose=None, insulin_route=None, laboratory_result_id=None,
                 result_datetime= None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-blood_glucose.v1'
        self.test_name = test_name or None
        self.specimen_arch_detail = specimen_arch_detail or None
        self.diet_intake = diet_intake or 'at0.95'
        self.diet_duration = diet_duration or 'P'
        self.glucose_dose = glucose_dose or randint(3, 10)
        self.glucose_timing = glucose_timing or 'at0.104'
        self.insulin_dose = insulin_dose or randint(2, 10)
        self.insulin_route = insulin_route or 'at0.113'
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(BloodGlucose, self).__init__(archetype_id, archetype_dir)

    def build(self):
        bg_doc = self._load_file()

        if self.specimen_arch_detail: #decide about  handling an example of nested archetipe
            pass
        if self.diet_intake:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.90'][0]['items']['at0.91'] = \
                {"value" : self._get_dv_coded_text(self.diet_intake, 'at0.92' )}
        if self.diet_duration:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.90'][0]['items']['at0.96'] = \
                {'value' : self._get_dv_duration(self.diet_duration, 'H')}
        if self.glucose_dose:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.98'][0]['items']['at0.100'] = \
                {'value' : self._get_quantity(self.glucose_dose, 'gm')}
        if self.glucose_timing:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.98'][0]['items']['at0.99'] = \
                {'value': self._get_dv_coded_text(self.glucose_timing, 'at0.103')}
        if self.insulin_dose:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.107'][0]['items']['at0.110'] = \
                {'value' : self._get_quantity(self.insulin_dose, 'U')}
        if self.insulin_route:
            bg_doc['data']['at0001'][0]['events']['at0002']['state']['at0.89'][0]['items']['at0.107'][0]['items']['at0.111'] = \
                {'value' : self._get_dv_coded_text(self.insulin_route, 'at0.112')}
        if self.laboratory_result_id:
            bg_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            bg_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(bg_doc)


class BloodPressure(ArchetypeBuilder):

    def __init__(self, archetype_dir, systolic=None, diastolic=None, mean_arterial=None, pulse=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.blood_pressure.v1'
        self.systolic = systolic or randint(80, 250)
        self.diastolic = diastolic or randint(60, 100)
        self.mean_arterial = mean_arterial or randint(0, 1000)
        self.pulse = pulse or randint(0,1000)
        super(BloodPressure, self).__init__(archetype_id, archetype_dir)

    def build(self):
        bp_doc = self._load_file()

        if self.systolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0004'] =\
                {'value': self._get_quantity(self.systolic, 'mm[Hg]')}
        if self.diastolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0005'] =\
                {'value': self._get_quantity(self.diastolic, 'mm[Hg]')}
        if self.mean_arterial:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1006'] =\
                {'value': self._get_quantity(self.mean_arterial, 'mm[Hg]')}
        if self.pulse:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1007'] =\
                {'value': self._get_quantity(self.pulse, 'mm[Hg]')}
        return self.archetype_id, self._clean_archetype(bp_doc)


class FullBloodCount(ArchetypeBuilder):

    def __init__(self, archetype_dir, test_name=None, haemoglobin=None, mchc=None, mcv=None,
                 mch=None, lymphocytes=None, basophils=None, monocytes=None,
                 eosinophils=None, multimedia_representation=None,
                 laboratory_result_id=None, result_datetime=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-full_blood_count.v1'
        self.test_name = test_name or None
        self.haemoglobin = haemoglobin or randint(10, 40)
        self.mchc = mchc or randint(25, 50)
        self.mcv = mcv or randint(80, 100)
        self.mch = mch or randint(30, 40)
        self.lymphocytes = lymphocytes or round(uniform(2, 5), 2)
        self.basophils = basophils or round(uniform(1, 3), 2)
        self.monocytes = monocytes or round(uniform(0, 1.5), 2)
        self.eosinophils = eosinophils or randint(3, 7)
        self.multimedia_representation = multimedia_representation or None
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(FullBloodCount, self).__init__(archetype_id, archetype_dir)

    def build(self):
        fbc_doc = self._load_file()

        if self.test_name:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(self.test_name)}
        if self.haemoglobin:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(self.haemoglobin, 'gm/l')}
        if self.mchc:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(self.mchc, 'gm/l')}
        if self.mcv:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.8'] = \
                {'value' : self._get_quantity(self.mcv, 'fl')}
        if self.mch:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.9'] = \
                {'value' : self._get_quantity(self.mcv, 'pg')}
        if self.lymphocytes:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.16]'] = \
                {'value' : self._get_quantity(self.lymphocytes, '10*9/l')}
        if self.basophils:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.17]'] = \
                {'value' : self._get_quantity(self.basophils, '10*9/l')}
        if self.monocytes:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.18]'] = \
                {'value' : self._get_quantity(self.monocytes, '10*9/l')}
        if self.eosinophils:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.14'][0]['items']['at0078.19]'] = \
                {'value' : self._get_quantity(self.eosinophils, '10*9/l')}
        if self.multimedia_representation:
            fbc_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                [{'value' : self._get_dv_multimedia(self.multimedia_representation['media_type'], self.multimedia_representation['size'])}]
        if self.laboratory_result_id:
            fbc_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            fbc_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(fbc_doc)


class Lipids(ArchetypeBuilder):

    def __init__(self, archetype_dir, test_name=None, specimen_detail=None, total_cholesterol=None,
                 tryglicerides=None, hdl=None, ldl=None, hdl_ldl_ratio=None,
                 laboratory_result_id=None, result_datetime= None):

        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-lipids.v1'

        self.test_name = test_name or None
        self.specimen_detail = specimen_detail or None
        self.total_cholesterol = total_cholesterol or randint(150, 300)
        self.tryglicerides = tryglicerides or randint(140, 550)
        self.hdl = hdl or randint(50, 120)
        self.ldl = ldl or randint(50, 120)
        self.hdl_ldl_ratio = hdl_ldl_ratio or {'numerator': randint(1, 4), 'denominator': randint(1, 4)}
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(Lipids, self).__init__(archetype_id, archetype_dir)


    def build(self):
        lpd_doc = self._load_file()

        if self.test_name:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(self.test_name)}
        if self.specimen_detail:
            pass  #decide about  handling an example of nested archetipe
        if self.total_cholesterol:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.5'] = \
                {'value' : self._get_quantity(self.total_cholesterol, "mg/dl")}
        if self.tryglicerides:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(self.tryglicerides, "mg/dl")}
        if self.hdl:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(self.hdl, "mg/dl")}
        if self.ldl:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(self.ldl, "mg/dl")}
        if self.hdl_ldl_ratio:
            lpd_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.1'] = \
                {'value' : self._get_dv_proportion(self.hdl_ldl_ratio, 1)}
        if self.laboratory_result_id:
            lpd_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            lpd_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(lpd_doc)


class LiverFunction(ArchetypeBuilder):

    def __init__(self, archetype_dir, test_name=None, alp=None, total_bilirubin=None, direct_bilirubin=None, indirect_bilirubin=None, alt=None, \
                 ast=None, ggt=None, albumin=None, total_protein=None, laboratory_result_id = None, result_datetime= None ):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-liver_function.v1'

        self.test_name = test_name or None
        self.alp = alp or randint(30, 45)
        self.total_bilirubin = total_bilirubin or randint(1, 25)
        self.direct_bilirubin = direct_bilirubin or randint (1, 9)
        self.indirect_bilirubin = indirect_bilirubin or randint(10, 20)
        self.alt = alt or randint(5, 50)
        self.ast = ast or randint(10, 50)
        self.ggt = ggt or randint(10, 50)
        self.albumin = albumin or randint(30, 55)
        self.total_protein = total_protein or randint(40, 65)
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(LiverFunction, self).__init__(archetype_id, archetype_dir)

    def build(self):
        lvf_doc = self._load_file()

        if self.test_name:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(self.test_name)}
        if self.alp:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.2'] =\
                {'value' : self._get_quantity(self.alp, "U/l")}
        if self.total_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.4'] =\
                {'value' : self._get_quantity(self.total_bilirubin, "µmol/l")}
        if self.direct_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.11'] =\
                {'value' : self._get_quantity(self.direct_bilirubin, "µmol/l")}
        if self.indirect_bilirubin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.9'] =\
                {'value' : self._get_quantity(self.indirect_bilirubin, "µmol/l")}
        if self.alt:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.1'] =\
                {'value' : self._get_quantity(self.alt, "U/l")}
        if self.ast:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.3'] = \
                {'value' : self._get_quantity(self.ast, "U/l")}
        if self.ggt:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.3'] = \
                {'value' : self._get_quantity(self.ggt, "U/l")}
        if self.albumin:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.7'] = \
                {'value' : self._get_quantity(self.albumin, "gm/l")}
        if self.total_protein:
            lvf_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['0078.10'] = \
                {'value' : self._get_quantity(self.total_protein, "gm/l")}
        if self.laboratory_result_id:
            lvf_doc['protocol']['at0004'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            lvf_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(lvf_doc)

class Thyroid(ArchetypeBuilder):

    def __init__(self, archetype_dir,test_name=None, tsh=None, ft3= None, total_t3=None, ft4=None, total_t4=None, ft3_index=None, fti=None, \
                 placer_id=None, filler_id=None, laboratory_result_id=None, result_datetime=None ):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-thyroid.v1'
        self.test_name = test_name or None
        self.tsh = tsh or round(uniform(0, 4.5), 2)
        self.ft3 = ft3 or round(uniform(3,7), 2)
        self.total_t3 = total_t3 or round(uniform(3, 7), 2)
        self.ft4 = ft4 or round(uniform(3,20), 2)
        self.total_t4 = total_t4 or round(uniform(3, 20), 2)
        self.ft3_index = ft3_index or {'numerator': randint(1, 4), 'denominator': randint(1, 4)}
        self.fti = fti or {'numerator': randint(1, 4), 'denominator': randint(1, 4)}
        self.placer_id = placer_id or None
        self.filler_id = filler_id or None
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(Thyroid, self).__init__(archetype_id, archetype_dir)

    def build(self):
        thy_doc = self._load_file()
        if self.test_name:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] =\
                {'value' : self._get_dv_text(self.test_name)}
        if self.tsh:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(self.tsh, 'mIU/l')}
        if self.ft3:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(self.ft3, 'pmol/l')}
        if self.total_t3:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.8'] = \
                {'value' : self._get_quantity(self.total_t3, 'pmol/l')}
        if self.ft4:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(self.ft3, 'pmol/l')}
        if self.total_t4:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(self.ft3, 'pmol/l')}
        if self.ft3_index:
             thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.9'] = \
                 {'value' : self._get_dv_proportion(self.ft3_index, 1)}
        if self.fti:
            thy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.6'] = \
                 {'value' : self._get_dv_proportion(self.fti, 1)}
        if self.placer_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0062'] = {'value' : self.placer_id}
        if self.filler_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0063'] = {'value' : self.filler_id}
        if self.laboratory_result_id:
            thy_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            thy_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(thy_doc)

class UreaAndElectrolytes(ArchetypeBuilder):

    def __init__(self, archetype_dir,test_name=None, sodum=None, potassium=None, chloride=None, bicarbonate=None, urea=None, creatinine=None, \
                 sp_ratio=None, laboratory_result_id=None, result_datetime=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.lab_test-urea_and_electrolytes.v1'
        self.test_name = test_name or None
        self.sodum = sodum or randint(125, 150)
        self.potassium = potassium or round(uniform(3, 5.5), 2)
        self.chloride = chloride or randint(90, 120)
        self.bicarbonate = bicarbonate or randint(20, 30)
        self.urea = urea or round(uniform(1.5, 8), 2)
        self.creatinine = creatinine or round(uniform(0.1, 0.3), 2)
        self.sp_ratio = sp_ratio or {'numerator': randint(3, 4), 'denominator': randint(1, 4)}
        self.laboratory_result_id = laboratory_result_id or None
        self.result_datetime = result_datetime or None
        super(UreaAndElectrolytes, self).__init__(archetype_id, archetype_dir)

    def build(self):
        uae_doc = self._load_file()

        if self.test_name:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0005'] = \
                {'value' : self._get_dv_text(self.test_name)}
        if self.sodum:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.5'] = \
                {'value' : self._get_quantity(self.sodum, 'mmol/l')}
        if self.potassium:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.4'] = \
                {'value' : self._get_quantity(self.potassium, 'mmol/l')}
        if self.chloride:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.3'] = \
                {'value' : self._get_quantity(self.chloride, 'mmol/l')}
        if self.bicarbonate:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.2'] = \
                {'value' : self._get_quantity(self.bicarbonate, 'mmol/l')}
        if self.urea:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.1'] = \
                {'value' : self._get_quantity(self.bicarbonate, 'mmol/l')}
        if self.creatinine:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.7'] = \
                {'value' : self._get_quantity(self.bicarbonate, 'mmol/l')}
        if self.sp_ratio:
            uae_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0078.6'] = \
                {'value' : self._get_dv_proportion(self.sp_ratio, 1)}
        if self.laboratory_result_id:
            uae_doc['protocol']['at0004'][0]['items']['at0013'][0]['items']['at0068'] = {'value' : self._get_dv_text(self.laboratory_result_id)}
        if self.result_datetime:
            uae_doc['protocol']['at0004'][0]['items']['at0075'] = {'value' : self._get_dv_date_time(self.result_datetime)}
        return self.archetype_id, self._clean_archetype(uae_doc)

class UrinAnalysis(ArchetypeBuilder):

    def __init__(self, archetype_dir,glucose=None, protein=None, bilirubin=None, ketones=None, blood=None, ph=None, comments=None  ):
        archetype_id = 'openEHR-EHR-OBSERVATION.urinalysis.v1'
        self.glucose = glucose or choice(['at0115', 'at0116', 'at0117', 'at0118', 'at0119', 'at0120'])
        self.protein = protein or choice(['at0096', 'at0097', 'at0098', 'at0099', 'at0100', 'at0101'])
        self.bilirubin = bilirubin or choice(['at0121', 'at0122', 'at0123', 'at0124'])
        self.ketones = ketones or choice(['at0109', 'at0110', 'at0111', 'at0112', 'at0113', 'at0114'])
        self.blood = blood or choice(['at0102', 'at0103', 'at0104', 'at0105', 'at0106', 'at0107', 'at0108'])
        self.ph = ph or choice(['at0127', 'at0128', 'at0129', 'at0130', 'at0131', 'at0132','at0133', 'at0134', 'at0176', 'at0177', 'at0179'])
        self.comments = comments or None
        super(UrinAnalysis, self).__init__(archetype_id, archetype_dir)

    def build (self):
        ualy_doc = self._load_file()

        if self.glucose:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0050'] = \
                {'value' : self.glucose}
        if self.protein:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0095'] = \
                {'value' : self.protein}
        if self.bilirubin:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0062'] = \
                {'value' : self.bilirubin}
        if self.ketones:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0037'] = \
                {'value' : self.ketones}
        if self.blood:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0032'] = \
                {'value' : self.blood}
        if self.ph:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0126'] = \
                {'value' : self.ph}
        if self.comments:
            ualy_doc['data']['at0001'][0]['events']['at0002']['data']['at0003'][0]['items']['at0030'] = \
                {'value' : self._get_dv_text(self.comments)}
        return self.archetype_id, self._clean_archetype(ualy_doc)


class Composition(ArchetypeBuilder):
    def __init__(self, archetype_dir, children, label):
        archetype_id = 'openEHR-EHR-COMPOSITION.encounter.v1.%s' % label
        self.archetype_file_name = 'openEHR-EHR-COMPOSITION.encounter.v1'
        super(Composition, self).__init__(archetype_id, archetype_dir)
        self.children = children

    def build(self):
        doc = self._load_file()
        doc['context']['event_context']['other_context']['at0001'][0]['items']['at0002'] = self.children
        return self.archetype_id, doc

    def _load_file(self):
        path = '/'.join([self.archetype_dir, self.archetype_file_name])
        with open("%s.json" % path) as f:
            doc = json.loads(f.read())

        try:
            doc = doc['archetype_details']
        except KeyError:
            raise Exception("Invalid archetype file: %s" % self.archetype_id)
        else:
            doc = decode_dict(doc)

        return doc


BUILDERS = {
    'blood_pressure' : BloodPressure,
    'blood_glucose' : BloodGlucose,
    'full_blood_count' : FullBloodCount,
    'lipids' : Lipids,
    'liver_function': LiverFunction,
    'thyroid' : Thyroid,
    'urea_and_electrolytes' : UreaAndElectrolytes,
    'urin_analysis' : UrinAnalysis,
    'composition' : Composition
}


def get_builder(name):
    return BUILDERS.get(name, None)
