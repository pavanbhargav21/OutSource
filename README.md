<template>
  <v-container>
    <v-card flat>
      <v-card-text>
        <v-form ref="form" v-model="isFormValid">
          <v-row align="center">
            <v-col cols="12" md="6">
              <v-autocomplete
                v-model="form.workflow_name"
                :items="workflowNames.map(workflow => workflow.workflow_name)"
                label="Please specify the workflow name"
                placeholder="Type"
                prepend-icon="mdi-database-arrow-up"
                solo
                dense
                return-object
                class="custom-font"
                allow-input
                :rules="[v => !!v || 'Workflow name is required']"
              >
                <template v-slot:prepend>
                  <span class="custom-label">Workflow Name: </span>
                </template>
                <template v-slot:append>
                  <v-btn icon @click="openDialog" color="white">
                    <v-icon class="font-size-40" color="black">mdi-plus-circle</v-icon>
                  </v-btn>
                </template>
              </v-autocomplete>
            </v-col>
          </v-row>

          <v-row align="center">
            <v-col cols="12" md="6">
              <v-text-field
                class="custom-font"
                v-model="form.url"
                label="Please specify the workflow URL"
                prepend-icon="mdi-link-variant"
                :rules="[v => !!v || 'URL is required']"
              >
                <template v-slot:prepend>
                  <span class="custom-label">Workflow URL: </span>
                </template>
              </v-text-field>
            </v-col>

            <v-col cols="12" md="6">
              <v-select
                v-model="form.environment"
                :items="dropdownItems"
                label="Select an option"
                prepend-icon="mdi-rotate-orbit"
                :rules="[v => !!v || 'Environment is required']"
                class="custom-font"
              >
                <template v-slot:prepend>
                  <span class="custom-label">Environment: </span>
                </template>
              </v-select>
            </v-col>
          </v-row>

          <v-row align="center">
            <v-col cols="12" md="6">
              <v-text-field
                class="custom-font"
                v-model="form.titles"
                label="Page titles (Comma Separated)"
                prepend-icon="mdi-page-next"
                :rules="[v => !!v || 'Titles are required']"
              >
                <template v-slot:prepend>
                  <span class="custom-label">Window Titles: </span>
                </template>
              </v-text-field>
            </v-col>
          </v-row>

          <div class="text-center">
            <v-btn color="primary" @click="submitForm" :disabled="!isFormValid">Submit</v-btn>
          </div>
        </v-form>

        <!-- Dialog content... -->

      </v-card-text>
    </v-card>
  </v-container>
</template>

<script>
import axios from '../axios';
import EventBus from '../eventBus';

export default {
  data() {
    return {
      isFormValid: false,
      form: {
        workflow_name: '',
        url: '',
        environment: '',
        titles: '',
      },
      workflowNames: [],
      newWorkflow: {
        workflow_name: '',
        system: '',
      },
      dropdownItems: ['UAT', 'Testing', 'Production'],
      dialog: false
    };
  },
  created() {
    this.fetchWorkflowNames();
  },
  methods: {
    // ... existing methods ...
  }
};
</script>

<style scoped>
.custom-font {
  font-family: 'Gill Sans', sans-serif;
  font-size: 14px;
}

.custom-label {
  font-family: 'Gill Sans', sans-serif;
  font-weight: bold;
  font-size: 14px;
}

.font-size-40 {
  font-size: 40px;
}

.text-center {
  display: flex;
  justify-content: center;
  margin-top: 20px;
}

.v-text-field,
.v-select,
.v-autocomplete {
  width: 100%;
}
</style>

<style>
.v-select__selection,
.v-select__selection-text,
.v-list-item__title {
  font-family: 'Gill Sans', sans-serif !important;
  font-size: 14px !important;
}
</style>