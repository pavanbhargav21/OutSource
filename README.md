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
                :rules="[v => !!v || 'Workflow name is required']"
                class="custom-font"
                :allow-custom
                @input="handleCustomInput"
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

          <!-- Other form fields remain unchanged -->

          <v-btn color="primary" @click="submitForm">Submit</v-btn>
        </v-form>

        <!-- Dialog code remains unchanged -->
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
          // Ensure the workflow name is saved if it's a new custom entry
          if (typeof this.form.workflow_name === 'string') {
            const isExisting = this.workflowNames.some(workflow => workflow.workflow_name === this.form.workflow_name);
            if (!isExisting) {
              await axios.post('/api/workflows/', { workflow_name: this.form.workflow_name });
              // Refresh the dropdown list
              this.fetchWorkflowNames();
            }
          }
          await axios.post('/api/whitelists/', this.form);
          alert('Form submitted successfully');
          this.resetForm();
          EventBus.$emit('workflowconfig-added');
        } catch (error) {
          console.error(error);
        }
      }
    },
    handleCustomInput(value) {
      // Handle custom input here if needed
      this.form.workflow_name = value;
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