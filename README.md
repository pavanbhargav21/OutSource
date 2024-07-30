<template>
  <v-container>
    <v-card flat>
      <v-card-text>
        <v-form ref="form">
          <v-row>
            <v-col cols="6" md="8">
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
                :allow-custom
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

          <v-row>
            <v-col cols="7">
              <!-- Workflow URL Field -->
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

            <v-col cols="5">
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

          <v-row>
            <v-col cols="6">
              <!-- Window Titles Field -->
              <v-text-field
                class="custom-font"
                v-model="form.titles"
                label="Page titles (Comma Separated)"
                prepend-icon="mdi-page-next"
                style="width: 100%;"
                solo
                :rules="[v => !!v || 'Titles are required']"
              >
                <template v-slot:prepend>
                  <span class="custom-label">Window Titles: </span>
                </template>
              </v-text-field>
            </v-col>
          </v-row>

          <v-btn color="primary" @click="submitForm">Submit</v-btn>
        </v-form>

        <v-dialog v-model="dialog" max-width="1000">
          <v-card>
            <v-card-title>
              <span class="headline">Workflow Master</span>
            </v-card-title>
            <v-card-text>
              <v-form ref="newWorkflowForm">
                <v-text-field
                  v-model="newWorkflow.workflow_name"
                  label="Workflow name"
                  :rules="[v => !!v || 'Workflow Name is required']"
                  required
                  class="custom-font"
                ></v-text-field>
                <v-text-field
                  v-model="newWorkflow.system"
                  label="System name"
                  :rules="[v => !!v || 'System Name is required']"
                  required
                  class="custom-font"
                ></v-text-field>
              </v-form>
            </v-card-text>
            <v-card-actions>
              <v-btn color="success" @click="submitWorkflow">Save</v-btn>
              <v-spacer></v-spacer>
              <v-btn color="primary" @click="dialog = false">Close</v-btn>
            </v-card-actions>
          </v-card>
        </v-dialog>
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
    async submitForm() {
      const isFormValid = this.$refs.form.validate();
      if (isFormValid) {
        try {
          await axios.post('/api/whitelists/', this.form);
          alert('Form submitted successfully');
          this.resetForm();
          EventBus.$emit('workflowconfig-added');
        } catch (error) {
          console.error(error);
        }
      }
    },
    async submitWorkflow() {
      const isFormValid = this.$refs.newWorkflowForm.validate();
      if (isFormValid) {
        try {
          await axios.post('/api/workflows/', this.newWorkflow);
          this.dialog = false;
          alert('Workflow added successfully');
          this.resetNewWorkflowForm();
          this.fetchWorkflowNames();
        } catch (error) {
          console.error(error);
        }
      }
    },
    openDialog() {
      this.dialog = true;
    },
    resetForm() {
      this.form = {
        workflow_name: '',
        url: '',
        environment: '',
        titles: '',
      };
      this.$refs.form.reset();
    },
    resetNewWorkflowForm() {
      this.newWorkflow = {
        workflow_name: '',
        system: '',
      };
      this.$refs.newWorkflowForm.reset();
    },
    async fetchWorkflowNames() {
      try {
        const response = await axios.get('/api/workflows');
        this.workflowNames = response.data.map(workflow => ({
          workflow_name: workflow.workflow_name,
          id: workflow.id
        }));
      } catch (error) {
        console.error(error);
      }
    },
  }
};
</script>

<style>
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

.v-autocomplete .v-input__control .v-select__selections,
.v-text-field .v-input__control .v-text-field__input,
.v-select .v-input__control .v-select__selections {
  font-family: 'Gill Sans', sans-serif;
  font-size: 14px;
}
</style>