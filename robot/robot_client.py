# robot/robot_client.py
from api_client import AnemAPIClient


class HarApiClient:
    def __init__(self, api_client: AnemAPIClient):
        self.api_client = api_client

    def validate_candidate(self, wassit, nin):
        return self.api_client.validate_candidate(wassit, nin)

    def get_preinscription(self, preinscription_id):
        return self.api_client.get_pre_inscription_info(preinscription_id)

    def get_available_dates(self, structure_id, preinscription_id):
        return self.api_client.get_available_dates(structure_id, preinscription_id)

    def create_rendezvous(self, preinscription_id, demandeur_id, rdv_date, ccp, nom_ccp, prenom_ccp):
        return self.api_client.create_rendezvous(
            preinscription_id,
            ccp,
            nom_ccp,
            prenom_ccp,
            rdv_date,
            demandeur_id,
        )
